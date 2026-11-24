package tist.flightdelay

import java.time.{LocalDate, ZoneOffset}
import org.apache.spark.sql.{Row, SparkSession}
import scala.util.Try

/**
 * Tests déterministes sans données externes. Lancés via --self-test.
 * Ils vérifient des CAS LIMITES ; ils ne remplacent pas une exécution sur les vrais CSV.
 */
object RegressionChecks {

  private def localSeconds(day: LocalDate, hour: Int, minute: Int): Long =
    day.atTime(hour, minute).toEpochSecond(ZoneOffset.UTC)

  private def bulletin(airport: Int, day: LocalDate, hour: Int, minute: Int, temp: Double): WeatherSlot =
    WeatherSlot(
      airportId = airport,
      obsEpochDay = day.toEpochDay.toInt,
      obsEpochSec = localSeconds(day, hour, minute),
      temperatureF = temp,
      relativeHumidity = 60.0,
      windDirectionDeg = 180.0,
      windSpeedKt = 5.0,
      stationPressureInHg = 29.9,
      skyCondition = "CLR",
      skyConditionScore = 0.0,
      visibilityMiles = 10.0,
      weatherType = "NONE",
      missingCount = 0
    )

  /**
   * New York 14 h EST (UTC−5) -> Los Angeles 11 h PST (UTC−8).
   * Le bulletin LAX 13 h 52 est postérieur au départ réel : il est exclu.
   * Le bulletin LAX 10 h 52 est antérieur au départ : il est retenu.
   */
  def checkCrossTimezoneCausality(): Boolean = {
    val day = LocalDate.of(2012, 1, 15)
    val depNY = localSeconds(day, 14, 0)
    val arrLA = localSeconds(day, 14, 0) // même heure locale après 3 h de vol
    val flight = FlightRecord(
      originAirportId = 12953,
      destAirportId = 12892,
      dayOfWeek = 7,
      originEpochDay = day.toEpochDay.toInt,
      destEpochDay = day.toEpochDay.toInt,
      crsDepTimeMinutes = 840,
      crsArrTimeMinutes = 840,
      crsElapsedMinutes = 180.0,
      scheduledDepEpochSec = depNY,
      scheduledArrEpochSec = arrLA,
      arrDelayMinutes = 80.0,
      weatherDelayMinutes = 10.0,
      nasDelayMinutes = 60.0,
      originStandardUtcOffsetHours = -5,
      destStandardUtcOffsetHours = -8
    )
    val before = bulletin(12892, day, 10, 52, 22.0)
    val future = bulletin(12892, day, 13, 52, 99.0)
    val daily = DailyWeather(Array(before, future), Array(before.obsEpochSec, future.obsEpochSec))
    val cutoffLA = SpatioTemporalJoiner.destinationCutoffLocalSec(flight)
    val atDeparture = SpatioTemporalJoiner.findMatchingObservation(daily, arrLA, cutoffLA, strictCausalMode = true)
    val wholeWindow = SpatioTemporalJoiner.buildWeatherWindow(daily, arrLA, cutoffLA, strictCausalMode = true)
    val standard = SpatioTemporalJoiner.findMatchingObservation(daily, arrLA, cutoffLA, strictCausalMode = false)

    cutoffLA == localSeconds(day, 11, 0) &&
      atDeparture.temperatureF == 22.0 &&
      wholeWindow.forall(w => w.temperatureF != 99.0) &&
      standard.temperatureF == 99.0 // le standard reste volontairement rétrospectif
  }

  /**
   * Test d'intégration Spark : jointure complète de New York vers Los Angeles.
   * Une observation après le départ doit être absente de TOUT le vecteur météo causal.
   */
  def checkCrossTimezoneJoinWithSpark(spark: SparkSession): Boolean = {
    val day = LocalDate.of(2012, 1, 15)
    val epochDay = day.toEpochDay.toInt
    val flight = FlightRecord(
      originAirportId = 12953,
      destAirportId = 12892,
      dayOfWeek = 7,
      originEpochDay = epochDay,
      destEpochDay = epochDay,
      crsDepTimeMinutes = 840,
      crsArrTimeMinutes = 840,
      crsElapsedMinutes = 180.0,
      scheduledDepEpochSec = localSeconds(day, 14, 0),
      scheduledArrEpochSec = localSeconds(day, 14, 0),
      arrDelayMinutes = 80.0,
      weatherDelayMinutes = 5.0,
      nasDelayMinutes = 75.0,
      originStandardUtcOffsetHours = -5,
      destStandardUtcOffsetHours = -8
    )
    val observations = Seq(
      bulletin(12953, day, 13, 52, 30.0), // origine, avant le départ
      bulletin(12892, day, 10, 52, 22.0), // destination, avant le départ
      bulletin(12892, day, 13, 52, 99.0)  // destination, après le départ réel !
    )
    val flightsRdd = spark.sparkContext.parallelize(Seq(flight), 2)
    val weatherRdd = spark.sparkContext.parallelize(observations, 2)
    val causal = SpatioTemporalJoiner.joinFlightsAndWeather(flightsRdd, weatherRdd, 3, strictCausalMode = true).collect()
    causal.length == 1 && causal.head.destWeather(0).temperatureF == 22.0 &&
      causal.head.destWeather.forall(w => w.temperatureF != 99.0)
  }

  /** RG-IMP-04 : 2400 == 00:00 à J+1, y compris pour la feature dayOfWeek. */
  def checkMidnightDayOfWeek(): Boolean = {
    val atl = AirportTimezone.create(10397, 13874, -5)
    val lax = AirportTimezone.create(12892, 23174, -8)
    val row = Row.fromSeq(Seq(
      "2012-05-10", "10397", "12892", "2400", "60.0", "0", "0", "180", "5", "0"
    ))
    val parsed = DataPreprocessor.parseFlightRow(row, Map(10397 -> atl, 12892 -> lax), localClockMode = false)
    parsed != null && parsed.dayOfWeek == 5 &&
      parsed.crsDepTimeMinutes == 0 &&
      parsed.originStandardUtcOffsetHours == -5 && parsed.destStandardUtcOffsetHours == -8
  }

  /** Rejet des configurations illégales, plutôt que retours silencieux. */
  def checkInvalidConfig(): Boolean = {
    val cases = Seq(
      Array("--threshold", "20"),
      Array("--m", "14"),
      Array("--n", "-1"),
      Array("--repetitions", "0"),
      Array("--partitions", "0"),
      Array("--causal-mode", "NON_CAUSAL"),
      Array("--causal-mode", "CAUSAL", "--time-mode", "LOCAL")
    )
    val allRejected = cases.forall(args => Try(PipelineConfig.parseArgs(args)).isFailure)
    val accepted = PipelineConfig.parseArgs(Array("--causal-mode", "CAUSAL", "--m", "7", "--n", "7"))
    allRejected && accepted.strictCausalMode && !accepted.localClockMode
  }

  /** Vérifie les bascules DST dans l'horloge STANDARD de la météo. */
  def checkDstStandardBoundary(): Boolean = {
    val atl = AirportTimezone.create(10397, 13874, -5)
    val phx = AirportTimezone.create(14107, 23183, -7)
    val spring = LocalDate.of(2012, 3, 11)
    val autumn = LocalDate.of(2012, 11, 4)
    !DataPreprocessor.isUsDstAtStandardTime(spring, 119, atl) &&
       DataPreprocessor.isUsDstAtStandardTime(spring, 120, atl) &&
       DataPreprocessor.isUsDstAtStandardTime(autumn, 59, atl) &&
      !DataPreprocessor.isUsDstAtStandardTime(autumn, 60, atl) &&
      !DataPreprocessor.isUsDstAtStandardTime(spring, 600, phx)
  }

  /** Le filtre ne doit pas accepter de vol annulé si le flag est manquant. */
  def checkInvalidFlightFlags(): Boolean = {
    val atl = AirportTimezone.create(10397, 13874, -5)
    val lax = AirportTimezone.create(12892, 23174, -8)
    val airportMap = Map(10397 -> atl, 12892 -> lax)
    def flight(cancelled: String, delay: String): FlightRecord = {
      val row = Row.fromSeq(Seq(
        "2012-05-10", "10397", "12892", "1200", delay, cancelled, "0", "180", "5", "0"
      ))
      DataPreprocessor.parseFlightRow(row, airportMap, localClockMode = false)
    }
    flight(null, "60") == null && flight("x", "60") == null &&
      flight("0", "NaN") == null && flight("0", "Infinity") == null &&
      flight("0", "60") != null
  }
}
