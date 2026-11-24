package tist.flightdelay

import java.time.LocalDate
import java.util.TimeZone
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * FICHIER 6 SUR 6 : PROGRAMME PRINCIPAL
 *   Phase 1 : nettoyage des vols (FT) et de la météo (OT), jointure en 2 étapes (JT)
 *   Phase 2 : jeu cible D1..D4 et équilibrage 50 % / 50 % (75 % entraînement, 25 % test)
 *   Phase 3 : entraînement du Random Forest M_{d,m,n}
 *   Phase 4 : évaluation (R répétitions), comparaison des 5 modèles, suite de validation (RG-VAL)
 */
object TistFlightDelayApp {

  def main(args: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC")) // RG-IMP-01
    val config = PipelineConfig.parseArgs(args)
    val spark  = createSparkSession(config)
    try { if (config.runSelfTest) runSelfTest(spark) else runFullPipeline(spark, config) }
    finally spark.stop()
  }

  def createSparkSession(config: PipelineConfig): SparkSession = {
    val localTempDir = new java.io.File("target/spark-temp").getAbsolutePath
    new java.io.File(localTempDir).mkdirs()
    val spark = SparkSession.builder()
      .appName("TIST-Flight-Delay-Predictive-System")
      .master(config.sparkMaster)
      .config("spark.sql.session.timeZone", "UTC") // RG-IMP-01
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.local.dir", localTempDir)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  /** Exécute `block` et renvoie son résultat avec la durée en secondes. */
  private def timed[T](block: => T): (T, Double) = {
    val start = System.nanoTime()
    val result = block
    (result, (System.nanoTime() - start) / 1e9)
  }

  def runFullPipeline(spark: SparkSession, config: PipelineConfig): Unit = {
    val pipelineStartNs = System.nanoTime()
    val rule = "=" * 80
    println(rule)
    println(" SYSTÈME PRÉDICTIF DES RETARDS DE VOLS (TIST-FLIGHT-DELAY — BELCASTRO ET AL.)")
    println(rule)
    println(s" Fuseau session Spark (RG-IMP-01) : ${spark.conf.get("spark.sql.session.timeZone")}")
    println(s" Mode temporel (RG-IMP-02)        : ${if (config.localClockMode) "LOCAL_CLOCK" else "EXACT_LST_UTC"}")
    println(s" Mode de jointure (RG-JNT-05/08)  : ${if (config.strictCausalMode) "STRICT_CAUSAL_C01_F04" else "STANDARD_BELCASTRO"}")
    println(s" Jeu cible / Seuil Th             : ${config.targetDataset} / Th = ${config.thresholdMinutes} min")
    println(s" Spécification météo              : ${config.activeWeatherSpec.label}")
    println(s" Répétitions (RG-ECH-04)          : ${config.repetitions}")
    println(rule)

    // ---- Phase 1 : FT, OT et jointure JT ----
    val ((joinedRdd, totalJoinedCount), phase1Sec) = timed {
      val airportRef = DataPreprocessor.loadAirportReference(config.resolvedTimezoneFile)
      println(f"[Phase 1] Référentiel chargé : ${airportRef.byAirportId.size}%d aéroports, ${airportRef.byWban.size}%d stations WBAN.")
      val flights = DataPreprocessor.loadAndCleanFlightTable(spark, config.resolvedFlightsGlob, airportRef, config.localClockMode, config.resolvedWeatherGlob)
      val observations = DataPreprocessor.loadAndCleanObservationTable(spark, config.resolvedWeatherGlob, airportRef).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val joined = SpatioTemporalJoiner.joinFlightsAndWeather(flights, observations, config.numJoinPartitions, config.strictCausalMode)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
      val count = joined.count()
      observations.unpersist()
      (joined, count)
    }
    println(f"[Phase 1] Table jointe JT construite : $totalJoinedCount%,d vols enrichis (13 obs origine + 13 obs destination) en $phase1Sec%.2f s.")

    // ---- Phase 2 : jeu cible et équilibrage ----
    val ((cohorts, firstSplit), phase2Sec) = timed {
      printTargetDatasetStatsTable(BalancedSampler.computeTargetDatasetStats(joinedRdd, config.thresholdMinutes))
      val cohorts = BalancedSampler.prepareIndexedCohorts(BalancedSampler.filterTargetDataset(joinedRdd, config.targetDataset, config.thresholdMinutes))
      if (!config.runValidationSuite) joinedRdd.unpersist()
      (cohorts, BalancedSampler.createBalancedSplitFromIndexedCohorts(cohorts, config.seed))
    }
    println(f"[Phase 2] Échantillonnage équilibré 3:1 (parité 50/50) : Train = ${firstSplit.totalTrainCount}%,d, Test = ${firstSplit.totalTestCount}%,d en $phase2Sec%.2f s.")

    // ---- Phases 3 et 4 : R répétitions (RG-ECH-04) ----
    val spec = config.activeWeatherSpec
    var totalTrainTime, totalEvalTime = 0.0
    var firstRfModel: TrainedRandomForestModel = null

    val dtRuns = scala.collection.mutable.ArrayBuffer.empty[EvaluationMetrics]
    val allRuns = (1 to config.repetitions).map { rep =>
      val repSeed = if (rep == 1) config.seed else config.seed + rep * 10007L
      val split   = if (rep == 1) firstSplit else BalancedSampler.createBalancedSplitFromIndexedCohorts(cohorts, repSeed)

      val (model, trainSec) = timed(RandomForestPipeline.trainModel(spark, split.trainingSet, spec, config, repSeed))
      totalTrainTime += trainSec
      if (rep == 1) {
        firstRfModel = model
        println(f"[Phase 3] Modèle Random Forest ${spec.label} entraîné (${config.numTrees} arbres, prof. max ${config.maxDepth}, maxBins=${model.effectiveMaxBins}) en $trainSec%.2f s.")
      }

      val (metrics, evalSec) = timed {
        val predictions = RandomForestPipeline.predict(spark, model, split.testSet)
        RandomForestPipeline.evaluatePredictions(predictions, split.totalTrainCount, split.totalTestCount, trainSec)
      }
      totalEvalTime += evalSec
      // Le DT demandé par le professeur est entraîné sur le même split et les
      // mêmes encodeurs que le RF, mais uniquement si l'option est activée.
      if (config.evaluateDecisionTreeEveryRepetition) {
        val dtMetrics = RandomForestPipeline.evaluateDecisionTreeOnSplit(
          spark, split, spec, model.encoders, config, repSeed)
        dtRuns += dtMetrics
        println(f"  [Decision Tree $rep%2d/${config.repetitions}%2d] Acc=${dtMetrics.accuracy * 100.0}%6.2f%% | " +
          f"Rec_o=${dtMetrics.onTimeRecall * 100.0}%6.2f%% | Rec_d=${dtMetrics.delayRecall * 100.0}%6.2f%% | AUC=${dtMetrics.aucRoc}%.4f")
      }
      println(f"  [Répétition $rep%2d/${config.repetitions}%2d] Acc = ${metrics.accuracy * 100.0}%6.2f%% | Rec_o = ${metrics.onTimeRecall * 100.0}%6.2f%% | Rec_d = ${metrics.delayRecall * 100.0}%6.2f%% | AUC = ${metrics.aucRoc}%.4f (${trainSec + evalSec}%.2f s)")
      metrics
    }

    print(RandomForestPipeline.formatEvaluationReport(AggregatedEvaluationResult(config.targetDataset, config.thresholdMinutes, spec.label, allRuns)))
    if (dtRuns.nonEmpty) {
      val dtSummary = AggregatedEvaluationResult(config.targetDataset, config.thresholdMinutes, spec.label, dtRuns.toSeq)
      println("\n===== DECISION TREE Spark ML — mêmes tirages que le Random Forest =====")
      print(RandomForestPipeline.formatEvaluationReport(dtSummary))
    }

    if (config.compareModels && firstRfModel != null)
      RandomForestPipeline.compareAllModelsOnSplit(spark, firstSplit, firstRfModel, allRuns.head, config)

    cohorts.unpersist()
    if (config.runValidationSuite) runBenchmarkValidationSuite(spark, joinedRdd, config)
    joinedRdd.unpersist()

    printPhaseTimingTable(phase1Sec, phase2Sec, totalTrainTime, totalEvalTime, (System.nanoTime() - pipelineStartNs) / 1e9)
  }

  /** RG-VAL-01 à RG-VAL-04 : rejoue les expériences de l'article (option --validation-suite). */
  def runBenchmarkValidationSuite(spark: SparkSession, joinedRdd: RDD[JoinedFlightWeather], config: PipelineConfig): Unit = {
    def run(ds: String, th: Int, spec: WeatherFeatureSpec) = RandomForestPipeline.runRepeatedEvaluation(spark, joinedRdd, ds, th, spec, config)

    println("\n--- [RG-VAL-01] Performance selon le seuil Th in {15, 30, 45, 60, 90} sur D2 (m = n = 7) ---")
    for (th <- Seq(15, 30, 45, 60, 90)) {
      val r = run("D2", th, WeatherFeatureSpec.OptimumSymmetric7h)
      println(f"  Th = $th%2d min | Acc = ${r.meanAccuracy * 100.0}%6.2f%% | Rec_o = ${r.meanOnTimeRecall * 100.0}%6.2f%% | Rec_d = ${r.meanDelayRecall * 100.0}%6.2f%% | AUC = ${r.meanAucRoc}%.4f")
    }

    println("\n--- [RG-VAL-02 & RG-VAL-03] Comparaison des régimes d'observations météo sur D2 (Th = 60 min) ---")
    for (spec <- Seq(WeatherFeatureSpec.NoWeather, WeatherFeatureSpec.hourly(11, 0), WeatherFeatureSpec.hourly(0, 11), WeatherFeatureSpec.Subsampled3Hour, WeatherFeatureSpec.OptimumSymmetric7h)) {
      val r = run("D2", 60, spec)
      println(f"  ${spec.label}%-32s | Acc = ${r.meanAccuracy * 100.0}%6.2f%% | Rec_o = ${r.meanOnTimeRecall * 100.0}%6.2f%% | Rec_d = ${r.meanDelayRecall * 100.0}%6.2f%%")
    }

    println("\n--- [RG-VAL-04] Sensibilité au jeu de données cible (D1, D2, D3, D4) à Th = 60 min, m = n = 7 ---")
    for (ds <- Seq("D1", "D2", "D3", "D4")) {
      val r = run(ds, 60, WeatherFeatureSpec.OptimumSymmetric7h)
      println(f"  Jeu $ds | Acc = ${r.meanAccuracy * 100.0}%6.2f%% | Rec_o = ${r.meanOnTimeRecall * 100.0}%6.2f%% | Rec_d = ${r.meanDelayRecall * 100.0}%6.2f%% | AUC = ${r.meanAucRoc}%.4f")
    }
  }

  /** Vérifications rapides des règles (option --self-test). */
  def runSelfTest(spark: SparkSession): Unit = {
    val rule = "=" * 80
    println(rule)
    println(" AUTO-VALIDATION DES RÈGLES MÉTIER ET ALGORITHMIQUES (--self-test)")
    println(rule)
    val atl = AirportTimezone.create(10397, 13874, -5)
    val phx = AirportTimezone.create(14107, 23183, -7)
    val (maskA, maskB) = BalancedSampler.drawDisjointBitSets(100, 75, 25, 42L)
    val checks = Seq(
      (spark.conf.get("spark.sql.session.timeZone") == "UTC", "RG-IMP-01 : Fuseau UTC requis"),
      (DataPreprocessor.isUsDst(LocalDate.of(2012, 7, 15), 720, atl), "RG-IMP-02 : ATL en heure d'été en juillet"),
      (!DataPreprocessor.isUsDst(LocalDate.of(2012, 7, 15), 720, phx), "RG-IMP-02 : PHX exempté d'heure d'été"),
      (DataPreprocessor.computeFlightTimestamps(LocalDate.of(2012, 5, 10), 1440, 120.0, atl, phx, localClockMode = false).depMinuteOfDay == 0, "RG-IMP-04 : 2400 doit basculer à 00:00 (J+1)"),
      (DataPreprocessor.parseQclcdNumeric("M", -80, 135) == -999.0, "RG-IMP-05 : 'M' -> -999.0"),
      (DataPreprocessor.parseQclcdNumeric("VRB", 0, 360, isWindDir = true) == -999.0, "RG-IMP-05 : 'VRB' -> -999.0"),
      (DataPreprocessor.normalizeWeatherType("") == "NONE", "RG-IMP-06 : WeatherType vide -> NONE"),
      (DataPreprocessor.normalizeWeatherType(null) == "NONE", "RG-IMP-06 : WeatherType CSV null -> NONE"),
      (DataPreprocessor.normalizeWeatherType("M") == "MISSING", "RG-IMP-06 : WeatherType M -> MISSING"),
      (RegressionChecks.checkCrossTimezoneCausality(), "RG-IMP-08 : aucun bulletin futur NY-LA"),
      (RegressionChecks.checkCrossTimezoneJoinWithSpark(spark), "RG-IMP-08 : jointure Spark NY-LA causale"),
      (RegressionChecks.checkMidnightDayOfWeek(), "RG-IMP-04 : 2400 -> jour de semaine J+1"),
      (RegressionChecks.checkDstStandardBoundary(), "RG-IMP-02 : bascules DST en LST"),
      (RegressionChecks.checkInvalidConfig(), "Configuration invalide correctement rejetée"),
      (RegressionChecks.checkInvalidFlightFlags(), "RG-PRE-03 : rejeter flags et nombres non valides"),
      (DataPreprocessor.normalizeSkyCondition("") == "MISSING", "RG-IMP-06 : SkyCondition vide -> MISSING"),
      (maskA.cardinality() == 75 && maskB.cardinality() == 25 && !maskA.intersects(maskB), "RG-ECH-02..04 : Split 75/25 disjoint")
    )
    checks.foreach { case (ok, message) => require(ok, message) }
    println(" [SUCCÈS] Contrôles ciblés réussis (ne constituent pas une validation des 39 règles).")
    println(rule)
  }

  def printTargetDatasetStatsTable(s: TargetDatasetStats): Unit = {
    def line(name: String, count: Long) = println(f"   $name%-31s: $count%,10d (${s.shareOfD4(count)}%5.1f %% de D4)")
    println("-" * 80)
    println(f" Volumétrie des Jeux Cibles à Th = ${s.thresholdMinutes} min (Section 4.3, Tableau VI) :")
    println(f"   Vols À l'heure (< ${s.thresholdMinutes} min) : ${s.onTimeCount}%,10d")
    line("D1 (Isolation causale pure)", s.d1DelayedCount)
    line("D2 (Météo > 0 ou NAS >= Th)", s.d2DelayedCount)
    line("D3 (Météo > 0 ou NAS > 0)", s.d3DelayedCount)
    println(f"   D4 (Tous les vols retardés)    : ${s.d4DelayedCount}%,10d (100.0 %% de D4)")
    println("-" * 80)
  }

  def printPhaseTimingTable(p1: Double, p2: Double, p3: Double, p4: Double, tot: Double): Unit = {
    println("=" * 80)
    println(" CHRONOMÉTRAGE DES PHASES DU PIPELINE DISTRIBUÉ (SECTION 8.4, TABLEAU IX)")
    println("=" * 80)
    println(f" 1. Prétraitement & Transformation (FT, OT, JT) : $p1%8.2f s")
    println(f" 2. Création des données cibles & Échantillonnage: $p2%8.2f s")
    println(f" 3. Modélisation (Entraînement Random Forest)   : $p3%8.2f s")
    println(f" 4. Évaluation (Matrice de confusion & ROC)     : $p4%8.2f s")
    println("-" * 80)
    println(f" Total de bout en bout                          : $tot%8.2f s")
    println("=" * 80)
  }
}
