package tist.flightdelay

import scala.collection.mutable
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassificationModel, GBTClassifier, LogisticRegression, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * FICHIER 5 SUR 6 : VARIABLES, APPRENTISSAGE ET ÉVALUATION (Sections 4.3 et 5)
 *
 *   1. extractFeatureVector : un vol → 5 + 10 × m + 10 × n nombres (RG-MOD-01, RG-MOD-02)
 *   2. fitFeatureEncoders   : numérotation des catégories par taux de retard (RG-IMP-07)
 *   3. trainModel / predict : Random Forest de l'article (50 arbres)
 *   4. evaluatePredictions  : matrice de confusion, Acc, Rec_o, Rec_d, AUC-ROC
 *   5. compareAllModelsOnSplit : arbre, régression logistique, RF, GBT, ensemble RF + GBT
 */

/** Numéros des catégories ; une catégorie inconnue reçoit le dernier numéro. */
case class TrainedFeatureEncoders(
    originAirportToIndex: Map[Int, Int],
    numOriginAirportBins: Int,
    destAirportToIndex: Map[Int, Int],
    numDestAirportBins: Int,
    weatherTypeToFreqIndex: Map[String, Int],
    weatherTypeToCentroidIndex: Map[String, Int],
    numWeatherTypeBins: Int,
    skyConditionToFreqIndex: Map[String, Int] // numérotée par taux de retard, malgré son nom
) {
  def encodeOriginAirport(id: Int): Int          = originAirportToIndex.getOrElse(id, numOriginAirportBins - 1)
  def encodeDestAirport(id: Int): Int            = destAirportToIndex.getOrElse(id, numDestAirportBins - 1)
  def encodeWeatherTypeFreq(wt: String): Int     = weatherTypeToFreqIndex.getOrElse(wt, numWeatherTypeBins - 1)
  def encodeWeatherTypeCentroid(wt: String): Int = weatherTypeToCentroidIndex.getOrElse(wt, numWeatherTypeBins - 1)
  def encodeSkyConditionFreq(sky: String): Int   = skyConditionToFreqIndex.getOrElse(sky, skyConditionToFreqIndex.size)
}

case class TrainedRandomForestModel(rfModel: RandomForestClassificationModel, encoders: TrainedFeatureEncoders, weatherSpec: WeatherFeatureSpec, effectiveMaxBins: Int)

/** (nombre de vols, nombre de retards) d'une catégorie. */
case class CategoryCounter(total: Long = 0L, delayed: Long = 0L) {
  def addOne(isDelayed: Long): CategoryCounter       = CategoryCounter(total + 1L, delayed + isDelayed)
  def merge(other: CategoryCounter): CategoryCounter = CategoryCounter(total + other.total, delayed + other.delayed)
  def smoothedDelayRate: Double                      = (delayed + 2.5) / (total + 5.0) // lissé vers 0,5
}

case class MlInstance(label: Double, features: Vector)
case class PredictionScore(trueLabel: Double, predictedLabel: Double, probOnTime: Double)

/** Moyenne et écart-type (n − 1) des métriques sur les R répétitions (RG-ECH-04). */
case class AggregatedEvaluationResult(dataset: String, thresholdMinutes: Int, weatherSpecLabel: String, runs: Seq[EvaluationMetrics]) {
  def computeMean(values: Array[Double]): Double = if (values.isEmpty) 0.0 else values.sum / values.length
  def computeStdDev(values: Array[Double]): Double =
    if (values.length <= 1) 0.0
    else { val m = computeMean(values); math.sqrt(values.map(v => (v - m) * (v - m)).sum / (values.length - 1)) }

  private def values(f: EvaluationMetrics => Double): Array[Double] = runs.map(f).toArray
  def meanAccuracy: Double     = computeMean(values(_.accuracy))
  def stdAccuracy: Double      = computeStdDev(values(_.accuracy))
  def meanOnTimeRecall: Double = computeMean(values(_.onTimeRecall))
  def stdOnTimeRecall: Double  = computeStdDev(values(_.onTimeRecall))
  def meanDelayRecall: Double  = computeMean(values(_.delayRecall))
  def stdDelayRecall: Double   = computeStdDev(values(_.delayRecall))
  def meanAucRoc: Double       = computeMean(values(_.aucRoc))
  def stdAucRoc: Double        = computeStdDev(values(_.aucRoc))
}

object RandomForestPipeline {

  val NumFlightFeatures: Int         = 5
  val NumFeaturesPerWeatherSlot: Int = 10
  val RocHistogramBins: Int          = 1000

  // ==========================================================================
  // 1. Random Forest : entraînement (Fig. 5-a) et prédiction (Fig. 5-b)
  // ==========================================================================

  def trainModel(spark: SparkSession, trainingRdd: RDD[JoinedFlightWeather], weatherSpec: WeatherFeatureSpec, config: PipelineConfig, seed: Long): TrainedRandomForestModel = {
    val encoders = fitFeatureEncoders(trainingRdd) // appris sur l'entraînement uniquement
    val trainDf  = buildFeatureDataFrame(spark, trainingRdd, weatherSpec, encoders, trainPartitions(spark)).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val maxBins = math.max(64, config.maxBins)
      val rf = new RandomForestClassifier()
        .setNumTrees(config.numTrees).setMaxDepth(config.maxDepth).setMaxBins(maxBins)
        .setMaxMemoryInMB(128).setFeatureSubsetStrategy("sqrt").setSeed(seed)
      TrainedRandomForestModel(rf.fit(trainDf), encoders, weatherSpec, maxBins)
    } finally trainDf.unpersist()
  }

  def predict(spark: SparkSession, trainedModel: TrainedRandomForestModel, testRdd: RDD[JoinedFlightWeather]): DataFrame =
    trainedModel.rfModel.transform(buildFeatureDataFrame(spark, testRdd, trainedModel.weatherSpec, trainedModel.encoders, -1))

  private def trainPartitions(spark: SparkSession): Int = math.max(4, math.min(12, spark.sparkContext.defaultParallelism))

  // ==========================================================================
  // 2. Encodage des catégories (RG-IMP-07)
  // ==========================================================================

  /** Compteurs (vols, retards) par catégorie pour une partition. */
  case class EncoderPartitionStats(orig: Map[Int, CategoryCounter], dest: Map[Int, CategoryCounter], weatherType: Map[String, CategoryCounter], sky: Map[String, CategoryCounter])

  /**
   * Numérote aéroports, WeatherType et SkyCondition du moins au plus souvent en retard (taux lissé).
   * WeatherType reçoit aussi une numérotation par fréquence.
   */
  def fitFeatureEncoders(trainingRdd: RDD[JoinedFlightWeather]): TrainedFeatureEncoders = {
    val stats = trainingRdd.mapPartitions { iter =>
      val orig = mutable.HashMap.empty[Int, CategoryCounter]
      val dest = mutable.HashMap.empty[Int, CategoryCounter]
      val wt   = mutable.HashMap.empty[String, CategoryCounter]
      val sky  = mutable.HashMap.empty[String, CategoryCounter]
      def bump[K](m: mutable.HashMap[K, CategoryCounter], k: K, d: Long): Unit = m(k) = m.getOrElse(k, CategoryCounter()).addOne(d)

      for (jt <- iter) {
        val d = if (jt.label == 1.0) 1L else 0L
        bump(orig, jt.flight.originAirportId, d)
        bump(dest, jt.flight.destAirportId, d)
        for (slot <- jt.originWeather ++ jt.destWeather) { bump(wt, slot.weatherType, d); bump(sky, slot.skyCondition, d) }
      }
      Iterator.single(EncoderPartitionStats(orig.toMap, dest.toMap, wt.toMap, sky.toMap))
    }.reduce((a, b) => EncoderPartitionStats(mergeMaps(a.orig, b.orig), mergeMaps(a.dest, b.dest), mergeMaps(a.weatherType, b.weatherType), mergeMaps(a.sky, b.sky)))

    val origIdx = numeroterParRetard(stats.orig)
    val destIdx = numeroterParRetard(stats.dest)
    val typeIdx = numeroterParRetard(stats.weatherType, maxCategories = 480)
    TrainedFeatureEncoders(
      originAirportToIndex       = origIdx,
      numOriginAirportBins       = math.max(2, origIdx.size + 1),
      destAirportToIndex         = destIdx,
      numDestAirportBins         = math.max(2, destIdx.size + 1),
      weatherTypeToFreqIndex     = numeroterTextesParFrequence(stats.weatherType, maxCategories = 480),
      weatherTypeToCentroidIndex = typeIdx,
      numWeatherTypeBins         = math.max(2, typeIdx.size + 1),
      skyConditionToFreqIndex    = numeroterParRetard(stats.sky, maxCategories = stats.sky.size)
    )
  }

  /** Numéros 0 … N − 1 par taux de retard croissant (puis effectif décroissant, puis nom). */
  def numeroterParRetard[T: Ordering](compteurs: Map[T, CategoryCounter], maxCategories: Int = Int.MaxValue): Map[T, Int] =
    compteurs.toSeq.sortBy { case (k, c) => (c.smoothedDelayRate, -c.total, k) }.take(maxCategories).map(_._1).zipWithIndex.toMap

  /** Numéros 0 … N − 1 par fréquence décroissante (puis nom). */
  def numeroterTextesParFrequence(compteurs: Map[String, CategoryCounter], maxCategories: Int): Map[String, Int] =
    compteurs.toSeq.sortBy { case (k, c) => (-c.total, k) }.take(maxCategories).map(_._1).zipWithIndex.toMap

  def mergeMaps[T](a: Map[T, CategoryCounter], b: Map[T, CategoryCounter]): Map[T, CategoryCounter] =
    b.foldLeft(a) { case (acc, (k, c)) => acc.updated(k, acc.getOrElse(k, CategoryCounter()).merge(c)) }

  // ==========================================================================
  // 3. Vecteur de variables (RG-MOD-01, RG-MOD-02)
  // ==========================================================================

  def buildFeatureDataFrame(spark: SparkSession, instancesRdd: RDD[JoinedFlightWeather], weatherSpec: WeatherFeatureSpec, encoders: TrainedFeatureEncoders, numPartitions: Int = -1): DataFrame = {
    val bcEncoders = spark.sparkContext.broadcast(encoders)
    val (origHours, destHours) = (weatherSpec.originOffsets, weatherSpec.destOffsets)
    val rdd = if (numPartitions > 0 && instancesRdd.getNumPartitions > numPartitions) instancesRdd.coalesce(numPartitions, shuffle = false) else instancesRdd
    spark.createDataFrame(rdd.map(jt => MlInstance(jt.label, extractFeatureVector(jt, origHours, destHours, bcEncoders.value))))
  }

  /** 5 variables du vol + 10 variables par heure météo retenue au départ puis à l'arrivée. */
  def extractFeatureVector(jt: JoinedFlightWeather, originOffsets: Array[Int], destOffsets: Array[Int], encoders: TrainedFeatureEncoders): Vector = {
    val f = jt.flight
    val flightPart = Array[Double](encoders.encodeOriginAirport(f.originAirportId), encoders.encodeDestAirport(f.destAirportId),
      f.dayOfWeek, f.crsDepTimeMinutes, f.crsArrTimeMinutes)
    val weatherPart = originOffsets.flatMap(k => weatherSlotFeatures(jt.originWeather(k), encoders)) ++
      destOffsets.flatMap(k => weatherSlotFeatures(jt.destWeather(k), encoders))
    Vectors.dense(flightPart ++ weatherPart)
  }

  /** Les 10 variables d'un bulletin. */
  def weatherSlotFeatures(slot: WeatherSlot, encoders: TrainedFeatureEncoders): Array[Double] = Array(
    slot.temperatureF, slot.relativeHumidity, slot.windDirectionDeg, slot.windSpeedKt, slot.stationPressureInHg,
    slot.skyConditionScore, encoders.encodeSkyConditionFreq(slot.skyCondition), slot.visibilityMiles,
    encoders.encodeWeatherTypeFreq(slot.weatherType), encoders.encodeWeatherTypeCentroid(slot.weatherType))

  // ==========================================================================
  // 4. Évaluation (Tableau II, équations (1) et (2), courbe ROC)
  // ==========================================================================

  case class EvalPartitionCounts(tp: Long, fn: Long, fp: Long, tn: Long, posHist: Array[Long], negHist: Array[Long]) {
    def add(o: EvalPartitionCounts): EvalPartitionCounts = EvalPartitionCounts(tp + o.tp, fn + o.fn, fp + o.fp, tn + o.tn,
      posHist.zip(o.posHist).map { case (x, y) => x + y }, negHist.zip(o.negHist).map { case (x, y) => x + y })
  }

  def evaluatePredictions(predictionsDf: DataFrame, trainCount: Long, testCount: Long, elapsedSeconds: Double): EvaluationMetrics = {
    val scores = predictionsDf.select("label", "prediction", "probability").rdd.map { row =>
      val prediction = row.getDouble(1)
      val prob       = row.getAs[Vector](2)
      val probOnTime = if (prob != null && prob.size > 0) math.max(0.0, math.min(1.0, prob(0))) else if (prediction == 0.0) 1.0 else 0.0
      PredictionScore(row.getDouble(0), prediction, probOnTime)
    }
    computeMetricsFromScores(scores, trainCount, testCount, elapsedSeconds)
  }

  /** TP, FN, FP, TN (positif = à l'heure) et histogrammes de P(à l'heure) pour la courbe ROC. */
  def computeMetricsFromScores(scoresRdd: RDD[PredictionScore], trainCount: Long, testCount: Long, elapsedSeconds: Double): EvaluationMetrics = {
    val t = scoresRdd.mapPartitions { iter =>
      val c   = Array.fill(4)(0L) // tp, fn, fp, tn
      val pos = new Array[Long](RocHistogramBins + 1)
      val neg = new Array[Long](RocHistogramBins + 1)
      for (s <- iter) {
        val bucket    = math.round(s.probOnTime * RocHistogramBins).toInt
        val predOnTime = s.predictedLabel == 0.0
        if (s.trueLabel == 0.0) { pos(bucket) += 1; c(if (predOnTime) 0 else 1) += 1 }
        else { neg(bucket) += 1; c(if (predOnTime) 2 else 3) += 1 }
      }
      Iterator.single(EvalPartitionCounts(c(0), c(1), c(2), c(3), pos, neg))
    }.reduce(_ add _)
    EvaluationMetrics(ConfusionMatrix(t.tp, t.fn, t.fp, t.tn), computeAucFromHistograms(t.posHist, t.negHist), trainCount, testCount, elapsedSeconds)
  }

  /** AUC-ROC par la méthode des trapèzes, en parcourant les seuils du plus haut au plus bas. */
  def computeAucFromHistograms(posHist: Array[Long], negHist: Array[Long]): Double = {
    val (totalPos, totalNeg) = (posHist.sum.toDouble, negHist.sum.toDouble)
    if (totalPos <= 0.0 || totalNeg <= 0.0) return 0.5
    var (cumPos, cumNeg, prevTpr, prevFpr, area) = (0.0, 0.0, 0.0, 0.0, 0.0)
    for (b <- posHist.indices.reverse) {
      cumPos += posHist(b); cumNeg += negHist(b)
      val (tpr, fpr) = (cumPos / totalPos, cumNeg / totalNeg)
      area += (fpr - prevFpr) * (tpr + prevTpr) * 0.5
      prevTpr = tpr; prevFpr = fpr
    }
    math.max(0.0, math.min(1.0, area))
  }

  /** R répétitions indépendantes pour une configuration (utilisé par la suite de validation). */
  def runRepeatedEvaluation(spark: SparkSession, joinedRdd: RDD[JoinedFlightWeather], dataset: String, thresholdMinutes: Int, weatherSpec: WeatherFeatureSpec, config: PipelineConfig): AggregatedEvaluationResult = {
    val cohorts = BalancedSampler.prepareIndexedCohorts(BalancedSampler.filterTargetDataset(joinedRdd, dataset, thresholdMinutes))
    try {
      val runs = (1 to config.repetitions).map { rep =>
        val startNs = System.nanoTime()
        val repSeed = if (rep == 1) config.seed else config.seed + rep * 10007L
        val split   = BalancedSampler.createBalancedSplitFromIndexedCohorts(cohorts, repSeed)
        val predDf  = predict(spark, trainModel(spark, split.trainingSet, weatherSpec, config, repSeed), split.testSet)
        evaluatePredictions(predDf, split.totalTrainCount, split.totalTestCount, (System.nanoTime() - startNs) / 1e9)
      }
      AggregatedEvaluationResult(dataset, thresholdMinutes, weatherSpec.label, runs)
    } finally cohorts.unpersist()
  }

  def formatEvaluationReport(result: AggregatedEvaluationResult): String = {
    val lastRun = result.runs.last
    val cm      = lastRun.confusionMatrix
    f"""================================================================================
       | RAPPORT D'ÉVALUATION : Jeu ${result.dataset} | Seuil Th = ${result.thresholdMinutes} min | ${result.weatherSpecLabel}
       |================================================================================
       | Répétitions exécutées (RG-ECH-04) : ${result.runs.size}%d
       | Taille Training Set équilibré     : ${lastRun.trainCount}%,d tuples (50.0%% À l'heure / 50.0%% En retard)
       | Taille Test Set équilibré         : ${lastRun.testCount}%,d tuples (50.0%% À l'heure / 50.0%% En retard)
       |--------------------------------------------------------------------------------
       | Matrice de Confusion Binaire (Tableau II — Dernière répétition) :
       |   [Réel À l'heure (0.0)]  TP = ${cm.tp}%,10d  |  FN = ${cm.fn}%,10d
       |   [Réel En retard (1.0)]  FP = ${cm.fp}%,10d  |  TN = ${cm.tn}%,10d
       |--------------------------------------------------------------------------------
       | Indicateurs Normalisés de Performance (Section 8.2) :
       |   Exactitude globale (Acc)        : ${result.meanAccuracy * 100.0}%6.2f %% (± ${result.stdAccuracy * 100.0}%5.2f %%)
       |   Rappel vols à l'heure (Rec_o)   : ${result.meanOnTimeRecall * 100.0}%6.2f %% (± ${result.stdOnTimeRecall * 100.0}%5.2f %%)
       |   Rappel vols en retard (Rec_d)   : ${result.meanDelayRecall * 100.0}%6.2f %% (± ${result.stdDelayRecall * 100.0}%5.2f %%)
       |   Aire sous la courbe (AUC-ROC)   : ${result.meanAucRoc}%6.4f    (± ${result.stdAucRoc}%6.4f)
       |================================================================================
       |""".stripMargin
  }

  // ==========================================================================
  // 5. Comparaison des 5 modèles sur le même couple entraînement / test
  // ==========================================================================

  /**
   * Évalue l'arbre de décision Spark ML sur un tirage déjà construit. Les encodeurs
   * proviennent UNIQUEMENT du train, exactement comme pour le Random Forest.
   * Permet de publier une moyenne DT sur R tirages (--dt-every-repetition).
   */
  def evaluateDecisionTreeOnSplit(
      spark: SparkSession,
      split: BalancedTrainTestSplit,
      weatherSpec: WeatherFeatureSpec,
      encoders: TrainedFeatureEncoders,
      config: PipelineConfig,
      seed: Long
  ): EvaluationMetrics = {
    val startNs = System.nanoTime()
    val trainDf = buildFeatureDataFrame(spark, split.trainingSet, weatherSpec, encoders, trainPartitions(spark))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val testDf = buildFeatureDataFrame(spark, split.testSet, weatherSpec, encoders, trainPartitions(spark))
      .persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val dt = new DecisionTreeClassifier()
        .setMaxDepth(config.maxDepth)
        .setMaxBins(math.max(64, config.maxBins))
        .setSeed(seed)
      val predictions = dt.fit(trainDf).transform(testDf)
      val metrics = evaluatePredictions(predictions, split.totalTrainCount, split.totalTestCount, 0.0)
      metrics.copy(elapsedSeconds = (System.nanoTime() - startNs) / 1e9)
    } finally {
      trainDf.unpersist()
      testDf.unpersist()
    }
  }

  def compareAllModelsOnSplit(spark: SparkSession, split: BalancedTrainTestSplit, trainedRf: TrainedRandomForestModel, rfMetrics: EvaluationMetrics, config: PipelineConfig): Seq[ModelComparisonEntry] = {
    println("================================================================================================")
    println(" BANC D'ESSAI COMPARATIF DES 5 MODÈLES SUR LE MÊME COUPLE (TRAINING SET / TEST SET)")
    println("================================================================================================")

    val (trainCount, testCount) = (split.totalTrainCount, split.totalTestCount)
    val maxBins = trainedRf.effectiveMaxBins
    def features(rdd: RDD[JoinedFlightWeather]) =
      buildFeatureDataFrame(spark, rdd, trainedRf.weatherSpec, trainedRf.encoders, trainPartitions(spark)).persist(StorageLevel.MEMORY_AND_DISK)
    val trainDf = features(split.trainingSet)
    val testDf  = features(split.testSet)
    trainDf.count(); testDf.count()

    /** Entraîne et évalue un modèle en mesurant le temps total. */
    def timedEval(name: String)(fitAndPredict: => DataFrame): EvaluationMetrics = {
      val t0 = System.nanoTime()
      val predictions = fitAndPredict
      val m = evaluatePredictions(predictions, trainCount, testCount, (System.nanoTime() - t0) / 1e9)
      printModelLine(name, m)
      m
    }

    try {
      val dtMetrics = timedEval("1. Decision Tree (1 arbre)") {
        new DecisionTreeClassifier().setMaxDepth(config.maxDepth).setMaxBins(maxBins).setSeed(config.seed).fit(trainDf).transform(testDf)
      }
      val lrMetrics = timedEval("2. Logistic Regression (Linéaire)") {
        val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true).fit(trainDf)
        new LogisticRegression().setFeaturesCol("scaledFeatures").setMaxIter(100).setRegParam(0.01).setElasticNetParam(0.5)
          .fit(scaler.transform(trainDf)).transform(scaler.transform(testDf))
      }
      printModelLine("3. Random Forest (Article)", rfMetrics)

      var gbtModel: GBTClassificationModel = null
      val gbtMetrics = timedEval("4. Gradient Boosting (GBT)") {
        gbtModel = new GBTClassifier().setMaxIter(80).setMaxDepth(8).setStepSize(0.08).setSubsamplingRate(0.85)
          .setFeatureSubsetStrategy("all").setMaxBins(maxBins).setMaxMemoryInMB(128).setSeed(config.seed).fit(trainDf)
        gbtModel.transform(testDf)
      }

      val t5 = System.nanoTime()
      val ensembleMetrics = evaluateSoftVotingEnsemble(spark, testDf, trainedRf.rfModel, gbtModel, 0.45, 0.55, trainCount, testCount, 0.0)
        .copy(elapsedSeconds = (System.nanoTime() - t5) / 1e9)
      printModelLine("5. Ensemble Hybride (RF + GBT)", ensembleMetrics)

      val results = Seq(
        ModelComparisonEntry("1. Decision Tree (1 arbre)", "Arbre unique (CART)", dtMetrics),
        ModelComparisonEntry("2. Logistic Regression", "Linéaire (ElasticNet)", lrMetrics),
        ModelComparisonEntry("3. Random Forest (Article)", "Bagging (50 arbres)", rfMetrics),
        ModelComparisonEntry("4. Gradient Boosting (GBT)", "Boosting (80 itérations)", gbtMetrics),
        ModelComparisonEntry("5. Ensemble Hybride RF+GBT", "Bagging + Boosting", ensembleMetrics)
      )
      printComparisonSummaryTable(results, config)
      results
    } finally { trainDf.unpersist(); testDf.unpersist() }
  }

  /** Vote « doux » : moyenne pondérée de P(à l'heure) des deux modèles, seuil 0,5. */
  def evaluateSoftVotingEnsemble(spark: SparkSession, testDf: DataFrame, rfModel: RandomForestClassificationModel, gbtModel: GBTClassificationModel,
                                 rfWeight: Double, gbtWeight: Double, trainCount: Long, testCount: Long, elapsedSeconds: Double): EvaluationMetrics = {
    val (bcRf, bcGbt) = (spark.sparkContext.broadcast(rfModel), spark.sparkContext.broadcast(gbtModel))
    val scores = testDf.select("label", "features").rdd.map { row =>
      val x = row.getAs[Vector](1)
      val p = math.max(0.0, math.min(1.0, (rfWeight * bcRf.value.predictProbability(x)(0) + gbtWeight * bcGbt.value.predictProbability(x)(0)) / (rfWeight + gbtWeight)))
      PredictionScore(row.getDouble(0), if (p >= 0.5) 0.0 else 1.0, p)
    }
    computeMetricsFromScores(scores, trainCount, testCount, elapsedSeconds)
  }

  def printModelLine(name: String, m: EvaluationMetrics): Unit =
    println(f"  [$name%-31s] Acc = ${m.accuracy * 100.0}%6.2f%% | Rec_o = ${m.onTimeRecall * 100.0}%6.2f%% | Rec_d = ${m.delayRecall * 100.0}%6.2f%% | AUC = ${m.aucRoc}%.4f (${m.elapsedSeconds}%.2f s)")

  def printComparisonSummaryTable(entries: Seq[ModelComparisonEntry], config: PipelineConfig): Unit = {
    val rfAcc = entries.find(_.modelName.contains("Random Forest")).map(_.metrics.accuracy).getOrElse(0.0)
    println("------------------------------------------------------------------------------------------------")
    println(f" SYNTHÈSE COMPARATIVE DES 5 MODÈLES (Jeu ${config.targetDataset} | Seuil Th = ${config.thresholdMinutes} min | ${config.activeWeatherSpec.label})")
    println("------------------------------------------------------------------------------------------------")
    println(" Modèle                       | Famille                  | Exactitude |  Rec_o  |  Rec_d  | AUC-ROC | Écart / RF")
    println("------------------------------+--------------------------+------------+---------+---------+---------+-----------")
    for (e <- entries) {
      val m    = e.metrics
      val diff = (m.accuracy - rfAcc) * 100.0
      val sign = if (diff >= 0.0) "+" else ""
      println(f" ${e.modelName}%-28s | ${e.familyDescription}%-24s |  ${m.accuracy * 100.0}%6.2f %%  | ${m.onTimeRecall * 100.0}%5.2f %% | ${m.delayRecall * 100.0}%5.2f %% | ${m.aucRoc}%6.4f  | $sign$diff%5.2f %%")
    }
    println("================================================================================================")
  }
}
