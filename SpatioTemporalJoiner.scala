package tist.flightdelay

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * FICHIER 3 SUR 6 : JOINTURE DES VOLS AVEC LA MÉTÉO (Algorithme 1 de l'article)
 *
 * Pour chaque vol : 13 bulletins au départ (W_o, étape 1) puis 13 à l'arrivée (W_d, étape 2).
 *   RG-JNT-01 : clé composite (aéroport, jour, étiquette 0 = météo / 1 = vol, heure)
 *   RG-JNT-02 : partition par hachage de (aéroport, jour) uniquement
 *   RG-JNT-03 : tri → la météo du jour arrive avant les vols du jour
 *   RG-JNT-04 : bulletins copiés vers le jour voisin autour de minuit
 *   RG-JNT-05 : bulletin le plus proche de chaque heure cible
 *   RG-JNT-06 : deuxième étape à l'arrivée
 *   RG-IMP-08 : mode causal avec comparaison inter-fuseaux corrigée (horodatages LST)
 */

/** RG-JNT-01 / RG-JNT-03 : clé composite, triée par (aéroport, jour, étiquette, heure). */
case class CompositeJoinKey(airportId: Int, epochDay: Int, tableTag: Byte, timestampSec: Long)

object CompositeJoinKey {
  val TagObservationTable: Byte = 0 // météo ("OT") : passe en premier
  val TagFlightTable: Byte      = 1 // vol ("FT")

  implicit val ordering: Ordering[CompositeJoinKey] =
    Ordering.by(k => (k.airportId, k.epochDay, k.tableTag, k.timestampSec))
}

/** RG-JNT-02 : même (aéroport, jour) → même partition. */
class JoinKeyPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[CompositeJoinKey]
    Math.floorMod(MurmurHash3.finalizeHash(MurmurHash3.mixLast(k.airportId, k.epochDay), 2), numPartitions)
  }
}

/** Élément d'une partition : soit un bulletin météo, soit un vol (avec sa météo de départ à l'étape 2). */
sealed trait JoinElement
case class WeatherElement(weather: WeatherSlot) extends JoinElement
case class FlightElement(flight: FlightRecord, originWeather: Array[WeatherSlot] = Array.empty) extends JoinElement

/** Bulletins d'un (aéroport, jour), triés par heure, avec leurs heures à part pour la recherche dichotomique. */
case class DailyWeather(observations: Array[WeatherSlot], times: Array[Long])

object SpatioTemporalJoiner {

  val TenAmInSeconds: Long        = 10L * 3600L
  val TwoAmInSeconds: Long        = 2L * 3600L
  val MaxCausalStalenessSec: Long = 7200L // RG-IMP-08 : 2 heures

  /**
   * Convertit le départ (encodé dans l'horloge LST de l'origine) en LST de la
   * destination. Les deux valeurs comparées décrivent alors le MÊME instant.
   *
   * Exemple : New York 14:00 EST (UTC-5) = Los Angeles 11:00 PST (UTC-8).
   */
  def destinationCutoffLocalSec(f: FlightRecord): Long =
    f.scheduledDepEpochSec +
      (f.destStandardUtcOffsetHours.toLong - f.originStandardUtcOffsetHours.toLong) * 3600L

  /** Jointure en deux étapes (départ puis arrivée) → table JT. */
  def joinFlightsAndWeather(
      flightsRdd: RDD[FlightRecord],
      observationsRdd: RDD[WeatherSlot],
      numPartitions: Int,
      strictCausalMode: Boolean = false
  ): RDD[JoinedFlightWeather] = {
    val partitioner = new JoinKeyPartitioner(numPartitions)
    val cachedObs   = observationsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Pour reproduire la sélection historique en STANDARD, on exige de la météo
    // sur les deux jours locaux. En CAUSAL, le jour de référence à destination
    // est celui du départ réel, exprimé dans le fuseau standard de destination.
    val activeDays = flightsRdd.sparkContext.broadcast(
      cachedObs.flatMap { o =>
        if (strictCausalMode) createWeatherJoinKeys(o).map { case (key, _) => (key.airportId, key.epochDay) }
        else Seq((o.airportId, o.obsEpochDay))
      }.distinct().collect().toSet
    )
    val eligibleFlights = flightsRdd.filter { f =>
      val destDay = if (strictCausalMode) Math.floorDiv(destinationCutoffLocalSec(f), 86400L).toInt
                    else f.destEpochDay
      activeDays.value.contains((f.originAirportId, f.originEpochDay)) &&
        activeDays.value.contains((f.destAirportId, destDay))
    }

    val keyedWeather = cachedObs.flatMap(createWeatherJoinKeys)

    /** Une étape : clé des vols, union avec la météo, partitionnement + tri, association. */
    def joinStep(flights: RDD[FlightElement], atDestination: Boolean): RDD[JoinedFlightWeather] = {
      val keyedFlights = flights.map { e =>
        val f = e.flight
        val key =
          if (atDestination) {
            val day = if (strictCausalMode) Math.floorDiv(destinationCutoffLocalSec(f), 86400L).toInt else f.destEpochDay
            CompositeJoinKey(f.destAirportId, day, CompositeJoinKey.TagFlightTable, f.scheduledArrEpochSec)
          }
          else CompositeJoinKey(f.originAirportId, f.originEpochDay, CompositeJoinKey.TagFlightTable, f.scheduledDepEpochSec)
        (key, e: JoinElement)
      }
      keyedWeather.union(keyedFlights)
        .repartitionAndSortWithinPartitions(partitioner)
        .mapPartitions(iter => if (atDestination) joinDestinationPartition(iter, strictCausalMode) else joinOriginPartition(iter, strictCausalMode))
    }

    val withOriginWeather = joinStep(eligibleFlights.map(f => FlightElement(f)), atDestination = false)        // étape 1 : W_o
    joinStep(withOriginWeather.map(jt => FlightElement(jt.flight, jt.originWeather)), atDestination = true)   // étape 2 : W_d (RG-JNT-06)
  }

  /**
   * RG-JNT-04 : un bulletin émis à partir de 10h00 sert aussi aux vols du lendemain matin
   * (fenêtre de 12 h + 2 h de marge) ; un bulletin émis avant 02h00 sert aussi aux vols de la veille au soir.
   */
  def createWeatherJoinKeys(obs: WeatherSlot): Seq[(CompositeJoinKey, JoinElement)] = {
    val element   = WeatherElement(obs)
    val secOfDay  = obs.obsEpochSec - obs.obsEpochDay * 86400L
    val key       = CompositeJoinKey(obs.airportId, obs.obsEpochDay, CompositeJoinKey.TagObservationTable, obs.obsEpochSec)
    val extraDays = if (secOfDay >= TenAmInSeconds) Seq(1) else if (secOfDay <= TwoAmInSeconds) Seq(-1) else Nil
    (key, element) +: extraDays.map(d => (key.copy(epochDay = obs.obsEpochDay + d), element))
  }

  /**
   * RG-JNT-03 : lit une partition triée. Les bulletins de chaque (aéroport, jour) sont rangés en premier,
   * puis chaque vol est associé à ce tableau. Un vol sans bulletin du jour lui-même est écarté.
   */
  def groupFlightsWithDailyWeather(
      iter: Iterator[(CompositeJoinKey, JoinElement)],
      allowAdjacentDayOnly: Boolean = false
  ): Iterator[(FlightElement, DailyWeather)] = {
    val matched  = new ArrayBuffer[(FlightElement, DailyWeather)]()
    val buffer   = new ArrayBuffer[WeatherSlot]()
    var group    = (-1, Int.MinValue)
    var hasToday = false
    var daily: DailyWeather = null

    for ((key, elem) <- iter) {
      if ((key.airportId, key.epochDay) != group) { // nouveau groupe (aéroport, jour)
        group = (key.airportId, key.epochDay)
        buffer.clear(); hasToday = false; daily = null
      }
      elem match {
        case WeatherElement(obs) =>
          if (obs.obsEpochDay == key.epochDay) hasToday = true
          // RG-IMP-05 : deux bulletins à la même minute → on garde le plus complet
          if (buffer.nonEmpty && buffer.last.obsEpochSec == obs.obsEpochSec) buffer(buffer.length - 1) = DataPreprocessor.resolveSameMinute(buffer.last, obs)
          else buffer += obs
        case flight: FlightElement =>
          if (daily == null) daily = DailyWeather(buffer.toArray, buffer.map(_.obsEpochSec).toArray)
          // En causal, le dernier bulletin disponible peut venir exclusivement de J-1.
          if ((hasToday || allowAdjacentDayOnly) && daily.observations.nonEmpty) matched += ((flight, daily))
      }
    }
    matched.iterator
  }

  /** Étape 1 : W_o = bulletins de t_sd à t_sd − 12 h à l'aéroport de départ. */
  def joinOriginPartition(iter: Iterator[(CompositeJoinKey, JoinElement)], strictCausalMode: Boolean): Iterator[JoinedFlightWeather] =
    groupFlightsWithDailyWeather(iter, allowAdjacentDayOnly = strictCausalMode).map { case (e, daily) =>
      val f = e.flight
      JoinedFlightWeather(f, buildWeatherWindow(daily, f.scheduledDepEpochSec, f.scheduledDepEpochSec, strictCausalMode), Array.empty)
    }

  /** Étape 2 : W_d = bulletins de t_sa à t_sa − 12 h à l'aéroport d'arrivée ; W_o est conservé. */
  def joinDestinationPartition(iter: Iterator[(CompositeJoinKey, JoinElement)], strictCausalMode: Boolean): Iterator[JoinedFlightWeather] =
    groupFlightsWithDailyWeather(iter, allowAdjacentDayOnly = strictCausalMode).map { case (e, daily) =>
      val f = e.flight
      val destDepartureCutoff = destinationCutoffLocalSec(f)
      JoinedFlightWeather(f, e.originWeather,
        buildWeatherWindow(daily, f.scheduledArrEpochSec, destDepartureCutoff, strictCausalMode))
    }

  /** Les 13 cases [t, t − 1 h, …, t − 12 h]. */
  def buildWeatherWindow(daily: DailyWeather, baseTargetSec: Long, scheduledDepSec: Long, strictCausalMode: Boolean): Array[WeatherSlot] =
    Array.tabulate(WeatherSlot.WindowSizeHours)(k => findMatchingObservation(daily, baseTargetSec - k * 3600L, scheduledDepSec, strictCausalMode))

  /**
   * RG-JNT-05 (mode standard) : bulletin le plus proche de la cible, avant ou après ; à égalité, le bulletin antérieur.
   * RG-IMP-08 (mode causal) : cible et heure de départ dans la MÊME horloge standard
   * locale de l'aéroport considéré. Le bulletin choisi doit précéder le vrai
   * décollage, et ne pas dater de plus de 2 h avant la cible ajustée.
   */
  def findMatchingObservation(daily: DailyWeather, rawTargetSec: Long, scheduledDepSec: Long, strictCausalMode: Boolean): WeatherSlot = {
    val obs = daily.observations
    if (obs.isEmpty) return WeatherSlot.MissingSlot

    val target = if (strictCausalMode) math.min(rawTargetSec, scheduledDepSec) else rawTargetSec
    val idx    = java.util.Arrays.binarySearch(daily.times, target)
    if (idx >= 0) return obs(idx) // bulletin exactement à l'heure cible

    val next = -(idx + 1) // indice du premier bulletin postérieur à la cible
    if (strictCausalMode) {
      if (next > 0 && target - obs(next - 1).obsEpochSec <= MaxCausalStalenessSec) obs(next - 1) else WeatherSlot.MissingSlot
    } else if (next == 0) obs.head
    else if (next == obs.length) obs.last
    else {
      val (before, after) = (obs(next - 1), obs(next))
      if (target - before.obsEpochSec <= after.obsEpochSec - target) before else after
    }
  }
}
