package tist.flightdelay

import java.io.File
import java.time.{DayOfWeek, LocalDate, ZoneOffset}
import java.time.temporal.TemporalAdjusters
import scala.io.Source
import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * FICHIER 2 SUR 6 : LECTURE ET NETTOYAGE DES DONNÉES
 *
 *   1. loadAirportReference         : référentiel des aéroports (wban_airport_timezone.csv)
 *   2. loadAndCleanFlightTable      : vols AOTP  → table FT  (RG-PRE-01..03, RG-CAU-01, RG-IMP-02..04)
 *   3. loadAndCleanObservationTable : météo QCLCD → table OT (RG-PRE-04, RG-PRE-05, RG-IMP-05..07)
 */

/** Référentiel : par numéro d'aéroport, et par station WBAN (une station peut couvrir plusieurs aéroports). */
case class AirportReferenceTable(byAirportId: Map[Int, AirportTimezone], byWban: Map[Int, Array[AirportTimezone]])

/** Jours et heures calculés pour un vol. */
case class FlightTimestamps(originEpochDay: Int, destEpochDay: Int, depMinuteOfDay: Int, arrMinuteOfDay: Int, schedDepSec: Long, schedArrSec: Long)

object DataPreprocessor {

  // RG-IMP-05 : bornes physiques des mesures météo
  val MinTempF: Double      = -80.0
  val MaxTempF: Double      = 135.0
  val MinHumidity: Double   = 0.0
  val MaxHumidity: Double   = 100.0
  val MinWindDir: Double    = 0.0
  val MaxWindDir: Double    = 360.0
  val MinWindSpeed: Double  = 0.0
  val MaxWindSpeed: Double  = 150.0
  val MinPressure: Double   = 18.0
  val MaxPressure: Double   = 32.0
  val MinVisibility: Double = 0.0
  val MaxVisibility: Double = 100.0

  private val Missing = -999.0

  // ==========================================================================
  // 1. Référentiel des aéroports
  // ==========================================================================

  def loadAirportReference(path: String): AirportReferenceTable = {
    val source = Source.fromFile(path, "UTF-8")
    val airports =
      try {
        source.getLines().map(_.trim)
          .filter(line => line.nonEmpty && !line.startsWith("AirportID")) // saute l'en-tête
          .map(_.split(","))
          .filter(_.length >= 3)
          .map(cols => (parseInt(cols(0)), parseInt(cols(1)), parseInt(cols(2))))
          .collect { case (id, wban, tz) if id >= 0 && wban >= 0 => AirportTimezone.create(id, wban, tz) }
          .toVector
      } finally source.close()

    AirportReferenceTable(
      byAirportId = airports.map(a => a.airportId -> a).toMap,
      byWban = airports.groupBy(_.wban).map { case (wban, list) => wban -> list.toArray }
    )
  }

  // ==========================================================================
  // 2. Vols AOTP → table FT
  // ==========================================================================

  def loadAndCleanFlightTable(
      spark: SparkSession,
      flightsGlob: String,
      airportRef: AirportReferenceTable,
      localClockMode: Boolean,
      weatherGlob: String = ""
  ): RDD[FlightRecord] = {
    val bcAirports = spark.sparkContext.broadcast(airportRef.byAirportId)
    val rawDf = spark.read.option("header", "true").option("inferSchema", "false")
      .csv(resolveMatchingFlightFiles(flightsGlob, weatherGlob): _*)

    val hasCrsArrTime = rawDf.columns.contains("CRS_ARR_TIME")
    val columns = Seq("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "ARR_DELAY_NEW",
      "CANCELLED", "DIVERTED", "CRS_ELAPSED_TIME", "WEATHER_DELAY", "NAS_DELAY") ++ (if (hasCrsArrTime) Seq("CRS_ARR_TIME") else Nil)

    rawDf.select(columns.head, columns.tail: _*).rdd
      .map(row => parseFlightRow(row, bcAirports.value, localClockMode, hasCrsArrTime))
      .filter(_ != null)
  }

  /** Contrôle une ligne de vol ; renvoie `null` si elle doit être écartée. */
  def parseFlightRow(row: Row, airportsById: Map[Int, AirportTimezone], localClockMode: Boolean, hasCrsArrTime: Boolean = false): FlightRecord = {
    def col(i: Int): String = row.getAs[String](i)

    // RG-PRE-01 / RG-PRE-02 : rejeter aussi les indicateurs manquants ou illisibles.
    // Le défaut 0 interprétait un indicateur absent comme « vol non annulé ».
    if (parseDouble(col(5), Double.NaN) != 0.0 || parseDouble(col(6), Double.NaN) != 0.0) return null

    // RG-PRE-03 : aéroports connus et champs obligatoires lisibles
    val origId = parseInt(col(1))
    val destId = parseInt(col(2))
    if (!airportsById.contains(origId) || !airportsById.contains(destId)) return null

    val flDate     = parseIsoDate(col(0))
    val crsDepMin  = parseHhmmToMinutes(col(3), allow2400 = true)
    val arrDelay   = parseDouble(col(4), Missing)
    val crsElapsed = parseDouble(col(7), Missing)
    if (flDate == null || crsDepMin < 0 || arrDelay == Missing || arrDelay.isNaN || arrDelay.isInfinite ||
        arrDelay < 0.0 || crsElapsed <= 0.0 || crsElapsed.isNaN || crsElapsed.isInfinite) return null

    // RG-IMP-02 à RG-IMP-04 : horaires alignés sur l'horloge de la météo
    val ts = computeFlightTimestamps(flDate, crsDepMin, crsElapsed, airportsById(origId), airportsById(destId), localClockMode)

    // Si CRS_ARR_TIME est fourni, il remplace la minute d'arrivée donnée au modèle
    val explicitArr = if (hasCrsArrTime && row.size > 10) parseHhmmToMinutes(col(10), allow2400 = true) else -1
    val crsArrMin   = if (explicitArr >= 0) explicitArr % 1440 else ts.arrMinuteOfDay

    FlightRecord(
      originAirportId      = origId,
      destAirportId        = destId,
      // RG-IMP-04 : « 2400 » correspond au lendemain ; il faut aussi corriger le jour de semaine.
      dayOfWeek            = flDate.plusDays(if (crsDepMin == 1440) 1L else 0L).getDayOfWeek.getValue,
      originEpochDay       = ts.originEpochDay,
      destEpochDay         = ts.destEpochDay,
      crsDepTimeMinutes    = ts.depMinuteOfDay,
      crsArrTimeMinutes    = crsArrMin,
      crsElapsedMinutes    = crsElapsed,
      scheduledDepEpochSec = ts.schedDepSec,
      scheduledArrEpochSec = ts.schedArrSec,
      arrDelayMinutes      = arrDelay,
      weatherDelayMinutes  = delayCauseOrZero(col(8)), // RG-CAU-01 : cause vide/invalide → 0
      nasDelayMinutes      = delayCauseOrZero(col(9)),
      // Offsets sauvegardés pour RG-IMP-08, sans modifier les horodatages LST du mode standard.
      originStandardUtcOffsetHours = airportsById(origId).timeZoneOffsetHours,
      destStandardUtcOffsetHours   = airportsById(destId).timeZoneOffsetHours
    )
  }

  /**
   * RG-IMP-02 à RG-IMP-04 : horaires du vol.
   * Le vol est en heure civile (avec heure d'été) ; la météo est en heure standard (LST) toute l'année.
   * « 2400 » = 1440 minutes : `plusMinutes` passe au lendemain à 00h00.
   */
  def computeFlightTimestamps(
      flDate: LocalDate,
      rawCrsDepMinutes: Int,
      crsElapsedMinutes: Double,
      originAirport: AirportTimezone,
      destAirport: AirportTimezone,
      localClockMode: Boolean
  ): FlightTimestamps = {
    def minuteOfDay(t: java.time.LocalDateTime): Int = t.getHour * 60 + t.getMinute

    // Départ : heure civile, puis heure standard (− 1 h si heure d'été)
    val depCivil = flDate.atStartOfDay().plusMinutes(rawCrsDepMinutes)
    val depLst   = depCivil.minusHours(if (isUsDst(depCivil.toLocalDate, minuteOfDay(depCivil), originAirport)) 1L else 0L)

    // Arrivée en heure standard : + durée prévue + écart de fuseau (RG-IMP-03)
    val arrLst = depLst.plusSeconds(math.round(crsElapsedMinutes * 60.0))
      .plusHours(destAirport.timeZoneOffsetHours - originAirport.timeZoneOffsetHours)
    // Une heure STANDARD à l'arrivée ne se teste pas comme une heure civile.
    // Début DST : 02:00 standard en mars ; fin DST : 01:00 standard en novembre.
    val arrCivil = arrLst.plusHours(if (isUsDstAtStandardTime(arrLst.toLocalDate, minuteOfDay(arrLst), destAirport)) 1L else 0L)

    val (dep, arr) = if (localClockMode) (depCivil, arrCivil) else (depLst, arrLst)
    FlightTimestamps(
      originEpochDay = dep.toLocalDate.toEpochDay.toInt,
      destEpochDay   = arr.toLocalDate.toEpochDay.toInt,
      depMinuteOfDay = minuteOfDay(depCivil),
      arrMinuteOfDay = minuteOfDay(arrCivil),
      schedDepSec    = dep.toEpochSecond(ZoneOffset.UTC),
      schedArrSec    = arr.toEpochSecond(ZoneOffset.UTC)
    )
  }

  /** RG-IMP-02 : heure d'été américaine, du 2e dimanche de mars à 2h00 au 1er dimanche de novembre à 2h00. */
  def isUsDst(date: LocalDate, localMinutes: Int, airport: AirportTimezone): Boolean = {
    if (!airport.observesDst) return false
    val day         = date.getDayOfMonth
    val firstSunday = date.`with`(TemporalAdjusters.firstInMonth(DayOfWeek.SUNDAY)).getDayOfMonth
    date.getMonthValue match {
      case m if m < 3 || m > 11 => false
      case m if m > 3 && m < 11 => true
      case 3 =>
        val start = firstSunday + 7
        day > start || (day == start && localMinutes >= 120)
      case _ => // novembre
        day < firstSunday || (day == firstSunday && localMinutes < 120)
    }
  }

  /**
   * Test DST pour un instant exprimé en heure STANDARD (LST), et non en heure civile.
   * Les instants de bascule ne sont pas les mêmes sur ces deux horloges :
   * - 2e dimanche de mars, à 02:00 LST : l'horloge civile saute de 02:00 à 03:00.
   * - 1er dimanche de novembre, à 01:00 LST : l'horloge civile recule de 02:00 à 01:00.
   */
  def isUsDstAtStandardTime(date: LocalDate, standardMinutes: Int, airport: AirportTimezone): Boolean = {
    if (!airport.observesDst) return false
    val month = date.getMonthValue
    if (month < 3 || month > 11) return false
    if (month > 3 && month < 11) return true
    val firstSunday = date.`with`(TemporalAdjusters.firstInMonth(DayOfWeek.SUNDAY)).getDayOfMonth
    val day = date.getDayOfMonth
    if (month == 3) {
      val secondSunday = firstSunday + 7
      day > secondSunday || (day == secondSunday && standardMinutes >= 120)
    } else {
      day < firstSunday || (day == firstSunday && standardMinutes < 60)
    }
  }

  // ==========================================================================
  // 3. Météo QCLCD → table OT
  // ==========================================================================

  def loadAndCleanObservationTable(spark: SparkSession, weatherGlob: String, airportRef: AirportReferenceTable): RDD[WeatherSlot] = {
    val bcWbanMap = spark.sparkContext.broadcast(airportRef.byWban)
    spark.read.option("header", "true").option("inferSchema", "false").csv(weatherGlob)
      .select("WBAN", "Date", "Time", "DryBulbFarenheit", "RelativeHumidity", "WindDirection",
        "WindSpeed", "StationPressure", "SkyCondition", "Visibility", "WeatherType") // RG-PRE-05
      .rdd
      .flatMap(row => parseWeatherRow(row, bcWbanMap.value))
  }

  /** Une ligne météo → un bulletin par aéroport rattaché à la station (aucun si la station est inconnue). */
  def parseWeatherRow(row: Row, airportsByWban: Map[Int, Array[AirportTimezone]]): Array[WeatherSlot] = {
    def col(i: Int): String = row.getAs[String](i)

    // RG-PRE-04 : seules les stations d'aéroport sont gardées
    val airports = airportsByWban.getOrElse(parseInt(col(0)), Array.empty[AirportTimezone])
    if (airports.isEmpty) return Array.empty

    val obsDate    = parseCompactDate(col(1))
    val obsMinutes = parseHhmmToMinutes(col(2), allow2400 = false)
    if (obsDate == null || obsMinutes < 0) return Array.empty
    val obsEpochDay = obsDate.toEpochDay.toInt
    val obsEpochSec = obsEpochDay * 86400L + obsMinutes * 60L

    // RG-IMP-05 : six mesures numériques
    val tempF      = parseQclcdNumeric(col(3), MinTempF, MaxTempF)
    val humidity   = parseQclcdNumeric(col(4), MinHumidity, MaxHumidity)
    val windDir    = parseQclcdNumeric(col(5), MinWindDir, MaxWindDir, isWindDir = true)
    val windSpeed  = parseQclcdNumeric(col(6), MinWindSpeed, MaxWindSpeed)
    val pressure   = parseQclcdNumeric(col(7), MinPressure, MaxPressure)
    val visibility = parseQclcdNumeric(col(9), MinVisibility, MaxVisibility)
    // RG-IMP-06 / RG-IMP-07 : deux descripteurs texte
    val sky         = normalizeSkyCondition(col(8))
    val weatherType = normalizeWeatherType(col(10))

    val missingCount = Seq(tempF, humidity, windDir, windSpeed, pressure, visibility).count(_ == Missing) +
      Seq(sky, weatherType).count(_ == WeatherSlot.CategoricalSentinel)

    airports.map(a => WeatherSlot(a.airportId, obsEpochDay, obsEpochSec, tempF, humidity, windDir, windSpeed,
      pressure, sky, encodeSkyConditionScore(sky), visibility, weatherType, missingCount))
  }

  /**
   * RG-IMP-05 : lit une mesure QCLCD. « M », vide, « VRB » (direction du vent) ou hors bornes → −999.
   * Les lettres de contrôle qualité en fin de valeur sont retirées (« 24s » → 24).
   */
  def parseQclcdNumeric(raw: String, minBound: Double, maxBound: Double, isWindDir: Boolean = false): Double = {
    val s = cleanString(raw)
    if (s.isEmpty || s.equalsIgnoreCase("M")) return Missing
    if (isWindDir && (s.equalsIgnoreCase("VRB") || s.equalsIgnoreCase("VR"))) return Missing
    val numberPart = s.take(s.lastIndexWhere(c => c.isDigit || c == '.') + 1)
    if (numberPart.isEmpty) return Missing
    val v = parseDouble(numberPart, Missing)
    if (v == Missing || v.isNaN || v.isInfinite || v < minBound || v > maxBound) Missing else v
  }

  /** RG-IMP-06 : WeatherType vide = aucun phénomène (« NONE ») ; seul « M » est manquant. */
  def normalizeWeatherType(raw: String): String =
    // Spark CSV représente en général un champ vide par null : ce cas signifie
    // « aucun phénomène signalé », et non « donnée météo inconnue ».
    // La sentinelle explicite « M » reste une donnée manquante.
    if (raw == null) "NONE"
    else raw.trim match {
      case s if s.equalsIgnoreCase("M") => WeatherSlot.CategoricalSentinel
      case ""                           => "NONE"
      case s                            => s
    }

  /** RG-IMP-06 : SkyCondition vide ou « M » = manquant (un ciel dégagé s'écrit « CLR »). */
  def normalizeSkyCondition(raw: String): String =
    if (raw == null) WeatherSlot.CategoricalSentinel
    else raw.trim match {
      case s if s.isEmpty || s.equalsIgnoreCase("M") => WeatherSlot.CategoricalSentinel
      case s                                        => s
    }

  /** Codes de couverture nuageuse, du plus grave au moins grave. */
  private val CoverRanks = Seq("VV" -> 5.0, "OVC" -> 4.0, "BKN" -> 3.0, "SCT" -> 2.0, "FEW" -> 1.0)

  /**
   * RG-IMP-07 : score de gravité du ciel = couverture la plus forte (FEW=1 … VV=5)
   * + un bonus < 1 d'autant plus grand que le plafond est bas.
   */
  def encodeSkyConditionScore(sky: String): Double = {
    if (sky == null || sky.isEmpty || sky == WeatherSlot.CategoricalSentinel || sky.equalsIgnoreCase("M")) return Missing
    val upper = sky.toUpperCase
    if (upper.contains("CLR") || upper.contains("SKC")) return 0.0

    var maxCover   = 0.0
    var minCeiling = 999.0 // en centaines de pieds
    for (token <- upper.split("\\s+"); (code, rank) <- CoverRanks.find { case (c, _) => token.startsWith(c) }) {
      maxCover = math.max(maxCover, rank)
      val digits = token.substring(code.length).takeWhile(_.isDigit)
      if (digits.nonEmpty) minCeiling = math.min(minCeiling, parseDouble(digits, 999.0))
    }
    val ceilingBonus =
      if (minCeiling >= 999.0) 0.0
      else math.max(0.0, math.min(0.99, (500.0 - math.min(500.0, minCeiling)) / 505.0))
    maxCover + ceilingBonus
  }

  /** RG-IMP-05 : deux bulletins à la même minute → on garde le plus complet (choix déterministe sinon). */
  def resolveSameMinute(a: WeatherSlot, b: WeatherSlot): WeatherSlot = {
    if (a.missingCount != b.missingCount) return if (a.missingCount < b.missingCount) a else b
    def text(s: String): String = if (s == WeatherSlot.CategoricalSentinel) "" else s
    val byType = text(a.weatherType).compareTo(text(b.weatherType))
    if (byType != 0) { if (byType > 0) a else b }
    else {
      val bySky = text(a.skyCondition).compareTo(text(b.skyCondition))
      if (bySky != 0) { if (bySky > 0) a else b }
      else {
        // En cas d'égalité sur la qualité et les descriptions, départager par
        // toutes les mesures ; le choix ne dépend plus de l'ordre des partitions.
        def signature(w: WeatherSlot): String = Seq(
          w.temperatureF, w.relativeHumidity, w.windDirectionDeg, w.windSpeedKt,
          w.stationPressureInHg, w.skyConditionScore, w.visibilityMiles
        ).mkString("|")
        if (signature(a).compareTo(signature(b)) >= 0) a else b
      }
    }
  }

  // ==========================================================================
  // 4. Petites fonctions de lecture
  // ==========================================================================

  /** « HHmm » → minutes depuis minuit ; « 2400 » → 1440 si autorisé ; −1 si invalide. */
  def parseHhmmToMinutes(raw: String, allow2400: Boolean = true): Int = {
    val s = cleanString(raw)
    if (s.isEmpty || !s.forall(_.isDigit)) return -1
    val hhmm = parseInt(s)
    if (allow2400 && hhmm == 2400) return 1440
    val (h, m) = (hhmm / 100, hhmm % 100)
    if (h >= 0 && h <= 23 && m >= 0 && m <= 59) h * 60 + m else -1
  }

  /** « AAAA-MM-JJ » → LocalDate (ou null). */
  def parseIsoDate(raw: String): LocalDate = {
    val s = cleanString(raw)
    if (s.length < 10) null
    else Try(LocalDate.of(s.substring(0, 4).toInt, s.substring(5, 7).toInt, s.substring(8, 10).toInt)).getOrElse(null)
  }

  /** « AAAAMMJJ » → LocalDate (ou null). */
  def parseCompactDate(raw: String): LocalDate = {
    val s = cleanString(raw)
    if (s.length != 8) null
    else Try(LocalDate.of(s.substring(0, 4).toInt, s.substring(4, 6).toInt, s.substring(6, 8).toInt)).getOrElse(null)
  }

  def cleanString(raw: String): String =
    if (raw == null) "" else raw.trim.stripPrefix("\"").stripSuffix("\"").trim

  def parseInt(raw: String): Int = Try(cleanString(raw).toInt).getOrElse(-1)

  def parseDouble(raw: String, defaultVal: Double): Double =
    Try(java.lang.Double.parseDouble(cleanString(raw))).getOrElse(defaultVal)

  /** Les causes sont des durées : NaN, infini et valeur négative sont invalides. */
  def delayCauseOrZero(raw: String): Double = {
    val value = parseDouble(raw, 0.0)
    if (value.isNaN || value.isInfinite || value < 0.0) 0.0 else value
  }

  /** Ne lit que les mois de vols pour lesquels un fichier météo existe (dossiers locaux uniquement). */
  def resolveMatchingFlightFiles(flightsGlob: String, weatherGlob: String): Seq[String] = {
    val weatherDir = new File(weatherGlob.stripSuffix("*hourly.txt"))
    val flightsDir = new File(flightsGlob.stripSuffix("*.csv"))
    val usable = flightsGlob.endsWith("*.csv") && weatherGlob.endsWith("*hourly.txt") && weatherDir.isDirectory && flightsDir.isDirectory
    if (!usable) return Seq(flightsGlob)

    val weatherMonths = weatherDir.listFiles().map(_.getName).filter(_.endsWith("hourly.txt")).map(_.take(6)).toSet
    val matched = flightsDir.listFiles()
      .filter(f => f.getName.endsWith(".csv") && weatherMonths.contains(f.getName.take(6)))
      .sortBy(_.getName).map(_.getPath).toSeq
    if (matched.nonEmpty) matched else Seq(flightsGlob)
  }
}
