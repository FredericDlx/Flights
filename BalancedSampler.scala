package tist.flightdelay

import java.util.BitSet
import scala.util.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * FICHIER 4 SUR 6 : JEUX CIBLES ET ÉQUILIBRAGE 50 % / 50 % (Section 4.2, Figure 4)
 *
 *   RG-ECH-01 : séparer vols en retard et vols à l'heure
 *   RG-ECH-02 : 75 % des retards pour l'entraînement, 25 % pour le test
 *   RG-ECH-03 : autant de vols à l'heure, tirés au hasard sans remise
 *   RG-ECH-04 : entraînement et test disjoints, tirage refait à chaque répétition
 */

case class BalancedTrainTestSplit(
    trainingSet: RDD[JoinedFlightWeather],
    testSet: RDD[JoinedFlightWeather],
    trainDelayedCount: Long,
    trainOnTimeCount: Long,
    testDelayedCount: Long,
    testOnTimeCount: Long
) {
  def totalTrainCount: Long = trainDelayedCount + trainOnTimeCount
  def totalTestCount: Long  = testDelayedCount + testOnTimeCount
}

/** Vols en retard et vols à l'heure, numérotés de 0 à N − 1 et gardés en mémoire. */
case class IndexedCohorts(indexedDelayed: RDD[(JoinedFlightWeather, Long)], indexedOnTime: RDD[(JoinedFlightWeather, Long)], delayedTotal: Long, onTimeTotal: Long) {
  def unpersist(): Unit = { indexedDelayed.unpersist(); indexedOnTime.unpersist() }
}

/** Nombre de vols dans D1 à D4 (Tableau VI). */
case class TargetDatasetStats(thresholdMinutes: Int, onTimeCount: Long, d1DelayedCount: Long, d2DelayedCount: Long, d3DelayedCount: Long, d4DelayedCount: Long) {
  def shareOfD4(count: Long): Double = if (d4DelayedCount == 0L) 0.0 else 100.0 * count / d4DelayedCount

  def add(o: TargetDatasetStats): TargetDatasetStats = TargetDatasetStats(thresholdMinutes,
    onTimeCount + o.onTimeCount, d1DelayedCount + o.d1DelayedCount, d2DelayedCount + o.d2DelayedCount,
    d3DelayedCount + o.d3DelayedCount, d4DelayedCount + o.d4DelayedCount)
}

object BalancedSampler {

  val TrainingSetFraction: Double = 0.75 // RG-ECH-02 : ratio 3:1
  val DelayedSeedSalt: Long       = 0x13579bdfL
  val OnTimeSeedSalt: Long        = 0x2468ace0L

  /** Écrit l'étiquette de chaque vol (getDatasetLabel) et retire les vols étiquetés −1. */
  def filterTargetDataset(joinedRdd: RDD[JoinedFlightWeather], dataset: String, thresholdMinutes: Int): RDD[JoinedFlightWeather] =
    joinedRdd.map(jt => jt.copy(label = jt.flight.getDatasetLabel(dataset, thresholdMinutes))).filter(_.label >= 0.0)

  /** Compte D1 à D4 en une seule lecture, avec la même règle que getDatasetLabel. */
  def computeTargetDatasetStats(joinedRdd: RDD[JoinedFlightWeather], thresholdMinutes: Int): TargetDatasetStats =
    joinedRdd.mapPartitions { iter =>
      val c = Array.fill(5)(0L) // à l'heure, D1, D2, D3, D4
      for (jt <- iter) {
        val f = jt.flight
        if (f.arrDelayMinutes < thresholdMinutes) c(0) += 1
        else {
          for ((ds, i) <- Seq("D1" -> 1, "D2" -> 2, "D3" -> 3) if f.getDatasetLabel(ds, thresholdMinutes) == 1.0) c(i) += 1
          c(4) += 1
        }
      }
      Iterator.single(TargetDatasetStats(thresholdMinutes, c(0), c(1), c(2), c(3), c(4)))
    }.reduce(_ add _)

  /** RG-ECH-01 : sépare les deux classes et numérote chaque vol (0, 1, 2, …). */
  def prepareIndexedCohorts(targetRdd: RDD[JoinedFlightWeather]): IndexedCohorts = {
    def indexed(label: Double) = targetRdd.filter(_.label == label).zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK_SER)
    val delayed = indexed(1.0)
    val onTime  = indexed(0.0)
    IndexedCohorts(delayed, onTime, delayed.count(), onTime.count())
  }

  /** RG-ECH-02 à RG-ECH-04 : entraînement (75 %) et test (25 %), chacun à 50 % / 50 %. */
  def createBalancedSplitFromIndexedCohorts(cohorts: IndexedCohorts, seed: Long): BalancedTrainTestSplit = {
    val minoritySize = math.min(cohorts.delayedTotal, cohorts.onTimeTotal)
    require(minoritySize >= 2L, s"Pas assez de vols pour créer un jeu d'entraînement et de test ($minoritySize)")
    val trainQuota = math.max(1L, math.round(minoritySize * TrainingSetFraction)).toInt
    val testQuota  = (minoritySize - trainQuota).toInt

    val (trainDelayedMask, testDelayedMask) = drawDisjointBitSets(cohorts.delayedTotal.toInt, trainQuota, testQuota, seed ^ DelayedSeedSalt)
    val (trainOnTimeMask, testOnTimeMask)   = drawDisjointBitSets(cohorts.onTimeTotal.toInt, trainQuota, testQuota, seed ^ OnTimeSeedSalt)

    val sc = cohorts.indexedDelayed.sparkContext
    /** Garde les vols dont le numéro est coché dans le masque. */
    def pick(cohort: RDD[(JoinedFlightWeather, Long)], mask: BitSet): RDD[JoinedFlightWeather] = {
      val bc = sc.broadcast(mask)
      cohort.filter { case (_, i) => bc.value.get(i.toInt) }.map(_._1)
    }

    BalancedTrainTestSplit(
      trainingSet       = pick(cohorts.indexedDelayed, trainDelayedMask).union(pick(cohorts.indexedOnTime, trainOnTimeMask)),
      testSet           = pick(cohorts.indexedDelayed, testDelayedMask).union(pick(cohorts.indexedOnTime, testOnTimeMask)),
      trainDelayedCount = trainQuota,
      trainOnTimeCount  = trainQuota,
      testDelayedCount  = testQuota,
      testOnTimeCount   = testQuota
    )
  }

  /**
   * Tire sans remise countA + countB numéros distincts parmi 0 … populationSize − 1 :
   * algorithme de Floyd, puis mélange de Fisher-Yates. Les countA premiers → masque A (entraînement),
   * les countB suivants → masque B (test).
   */
  def drawDisjointBitSets(populationSize: Int, countA: Int, countB: Int, seed: Long): (BitSet, BitSet) = {
    val total = countA + countB
    require(total <= populationSize, s"Impossible de tirer $total éléments parmi $populationSize")
    val rng = new Random(seed)

    // Floyd : `total` numéros distincts (tous les numéros si total = populationSize)
    val picked =
      if (total == populationSize) Array.range(0, populationSize)
      else {
        val taken = new BitSet(populationSize)
        Array.range(populationSize - total, populationSize).map { j =>
          val candidate = rng.nextInt(j + 1)
          val chosen    = if (taken.get(candidate)) j else candidate
          taken.set(chosen)
          chosen
        }
      }

    // Fisher-Yates : mélange des numéros tirés
    for (k <- total - 1 until 0 by -1) {
      val s = rng.nextInt(k + 1)
      val tmp = picked(k); picked(k) = picked(s); picked(s) = tmp
    }

    val (maskA, maskB) = (new BitSet(populationSize), new BitSet(populationSize))
    picked.take(countA).foreach(maskA.set)
    picked.drop(countA).foreach(maskB.set)
    (maskA, maskB)
  }
}
