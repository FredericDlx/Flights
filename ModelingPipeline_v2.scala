package flight

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

/**
 * MODÉLISATION — Belcastro et al. 2016, §4.2 à §5.
 * Entrée : table JT écrite par Flights_v2.
 *
 * 1. Manquants : null -> −999 (6 numériques) / "MISSING" (S, D)   [§4.1 : méthode non précisée]
 * 2. Classe : en retard si ARR_DELAY_NEW ≥ Th                      [§2]
 * 3. Datasets D2, D3, D4 (D1 impossible : causes absentes)         [§4.2]
 * 4. Équilibrage : retards 3:1 train/test + autant de vols à l'heure, sans remise   [fig. 4]
 * 5. Variables : aéroports, jour, heures prévues + m obs. au départ, n à l'arrivée  [§5]
 *    aéroports et D = catégories ; S = numéro par fréquence lu comme un nombre
 *    (contrainte Spark : ~4 300 valeurs de S, voir le rapport, tableau 8)
 * 6. Random Forest Spark, réglages par défaut (article muet), maxBins automatique   [§4.3]
 * 7. Acc, Rec_o, Rec_d (à l'heure = positif) + AUC ; moyenne sur 10 répétitions     [§2.4, §5]
 */
object ModelingPipeline_v2 {

  // ---------------- RÉGLAGES ----------------
  val InputPath = "data/cached/flights_weather_joined_13obs_v2.parquet"
  val Mode = "fig9"   // "test" | "fig6a" | "fig6b" | "fig7" | "tab8" | "fig8" | "fig9" | "all"
  val Reps = 10        // 10 répétitions comme l'article (§5)

  // ---------------- COLONNES ----------------
  val NumWeather: Seq[String] = WeatherPipeline_v2.NumericCols            // T, H, Wd, Ws, P, V
  val Sides = Seq("orig", "dest")
  val AllNumW: Seq[String] = for (s <- Sides; f <- NumWeather; k <- 0 until JoinPipeline_v2.NObs) yield s"${f}_${s}_$k"
  val AllCatW: Seq[String] = for (s <- Sides; f <- Seq("SkyCondition", "WeatherType"); k <- 0 until JoinPipeline_v2.NObs) yield s"${f}_${s}_$k"
  val Airports  = Seq("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID")             // catégories
  val FlightNum = Seq("day_of_week", "CRS_DEP_TIME", "CRS_ARR_TIME")      // nombres

  // ---------------- EXPÉRIENCES (§5, tableau VII) ----------------
  case class Exp(name: String, dataset: String, th: Int, origKs: Seq[Int], destKs: Seq[Int])
  def first(m: Int): Seq[Int] = 0 until m                                // m premières obs. : k = 0 … m−1
  val Ms = Seq(0, 1, 3, 5, 7, 9, 11)

  def experiments(mode: String): Seq[Exp] = mode match {
    case "test"  => Seq(Exp("test D2 Th=60 m=n=7", "D2", 60, first(7), first(7)))
    case "fig6a" => Ms.map(m => Exp(s"fig6a m=$m n=0", "D2", 60, first(m), Nil))
    case "fig6b" => Ms.map(n => Exp(s"fig6b m=0 n=$n", "D2", 60, Nil, first(n)))
    case "fig7"  => Ms.map(m => Exp(s"fig7 m=n=$m", "D2", 60, first(m), first(m)))
    case "tab8"  => Seq(Exp("tab8 k=0,3,6", "D2", 60, Seq(0, 3, 6), Seq(0, 3, 6)),
                        Exp("tab8 m=n=7", "D2", 60, first(7), first(7)))
    case "fig8"  => Seq(15, 30, 45, 60, 90).map(th => Exp(s"fig8 Th=$th", "D2", th, first(7), first(7)))
    case "fig9"  => Seq("D2", "D3", "D4").map(d => Exp(s"fig9 $d", d, 60, first(7), first(7)))
    case "all"   => Seq("fig6a", "fig6b", "fig7", "tab8", "fig8", "fig9").flatMap(experiments)
  }

  // 1. Manquants (une seule fois, après la jointure)
  def fillMissing(jt: DataFrame): DataFrame =
    jt.na.fill(-999.0, AllNumW).na.fill("MISSING", AllCatW)

  // 2–3. Classe + dataset cible : tous les vols à l'heure + les retards retenus par Di
  def target(df: DataFrame, dataset: String, th: Int): DataFrame = {
    val late = col("ARR_DELAY_NEW") >= th
    val keepLate: Column = dataset match {
      case "D2" => col("WEATHER_DELAY") > 0 || col("NAS_DELAY") >= th
      case "D3" => col("WEATHER_DELAY") > 0 || col("NAS_DELAY") > 0
      case "D4" => lit(true)
    }
    df.withColumn("label", when(late, 1.0).otherwise(0.0)).filter(!late || keepLate)
  }

  // 4. Équilibrage exact (fig. 4)
  def balance(t: DataFrame, seed: Long): (DataFrame, DataFrame) = {
    def shuffled(df: DataFrame, s: Long): DataFrame =
      df.withColumn("_r", rand(s)).withColumn("_pos", row_number().over(Window.orderBy(col("_r"), col("fid"))))
    val late   = shuffled(t.filter(col("label") === 1.0), seed)
    val onTime = shuffled(t.filter(col("label") === 0.0), seed + 7919L)
    val nLate  = late.count()
    val nTr    = math.round(nLate * 0.75)                                 // 3:1
    val train = late.filter(col("_pos") <= nTr).unionByName(onTime.filter(col("_pos") <= nTr))
    val test  = late.filter(col("_pos") > nTr)
      .unionByName(onTime.filter(col("_pos") > nTr && col("_pos") <= nLate))
    (train.drop("_r", "_pos"), test.drop("_r", "_pos"))
  }

  // 5–7. Encodage (appris sur le train), Random Forest, mesures
  def trainEval(train: DataFrame, test: DataFrame, e: Exp, seed: Long): Array[Double] = {
    val obs  = e.origKs.map(("orig", _)) ++ e.destKs.map(("dest", _))
    val wNum = for ((s, k) <- obs; f <- NumWeather) yield s"${f}_${s}_$k"
    val sky  = obs.map { case (s, k) => s"SkyCondition_${s}_$k" }
    val cats = Airports ++ obs.map { case (s, k) => s"WeatherType_${s}_$k" }   // vraies catégories
    val toIndex = cats ++ sky
    val idx  = toIndex.map(_ + "_idx")

    val indexer = new StringIndexer().setInputCols(toIndex.toArray).setOutputCols(idx.toArray)
      .setHandleInvalid("keep").fit(train)                                // valeur jamais vue -> "inconnu"
    val nCat    = cats.map(c => indexer.labelsArray(toIndex.indexOf(c)).length + 1)
    val maxBins = (32 +: nCat).max

    // S : on remplace l'étiquette "catégorie" par une étiquette neutre -> lu comme un nombre
    // (une étiquette VIDE serait ignorée par Spark)
    val numMeta = new MetadataBuilder().putString("encodage", "numero_par_frequence").build()
    def prep(df: DataFrame): DataFrame =
      sky.foldLeft(indexer.transform(df))((d, c) => d.withMetadata(c + "_idx", numMeta))
    val assembler = new VectorAssembler().setInputCols((FlightNum ++ wNum ++ idx).toArray).setOutputCol("features")

    val model = new RandomForestClassifier().setMaxBins(maxBins).setSeed(seed)
      .fit(assembler.transform(prep(train)))
    val pred = model.transform(assembler.transform(prep(test)))
      .select("label", "prediction", "rawPrediction").persist(StorageLevel.MEMORY_AND_DISK)

    def n(l: Double, p: Double): Double = pred.filter(col("label") === l && col("prediction") === p).count().toDouble
    val (tp, fn, fp, tn) = (n(0, 0), n(0, 1), n(1, 0), n(1, 1))           // tableau II : à l'heure = positif
    val auc = new BinaryClassificationEvaluator().evaluate(pred)
    pred.unpersist()
    Array((tp + tn) / (tp + tn + fp + fn), tp / (tp + fn), tn / (tn + fp), auc)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FlightDelayModeling_v2").master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.session.timeZone", "UTC")                         // comme Flights_v2
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")                                // sortie lisible

    println(s"\n=== MODÉLISATION v2 — mode $Mode, $Reps répétitions, mémoire ${Runtime.getRuntime.maxMemory / 1048576} Mo ===")
    val keep = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY") ++ Airports ++ FlightNum ++ AllNumW ++ AllCatW
    val base = fillMissing(spark.read.parquet(InputPath)).select(keep.map(col): _*)
      .withColumn("fid", monotonically_increasing_id()).persist(StorageLevel.MEMORY_AND_DISK)
    val nAll  = base.count()
    val nFull = base.na.drop(keep).count()
    println(s"Contrôle : $nAll vols, lignes avec un null restant = ${nAll - nFull}   ${if (nAll == nFull) "OK" else "ÉCART"}")

    val results = experiments(Mode).map { e =>
      val t = target(base, e.dataset, e.th).persist(StorageLevel.MEMORY_AND_DISK)
      val runs = (1 to Reps).map { r =>
        val (tr, te) = balance(t, 1000L + r)
        val (train, test) = (tr.persist(StorageLevel.MEMORY_AND_DISK), te.persist(StorageLevel.MEMORY_AND_DISK))
        if (r == 1) {                                                      // contrôle d'équilibrage
          def c(df: DataFrame, l: Double) = df.filter(col("label") === l).count()
          val (a, b, x, y) = (c(train, 0), c(train, 1), c(test, 0), c(test, 1))
          println(f"${e.name}%-22s train $a%d/$b%d  test $x%d/$y%d  ${if (a == b && x == y) "OK" else "ÉCART"}")
        }
        val m = trainEval(train, test, e, 1000L + r)
        train.unpersist(); test.unpersist()
        m
      }
      t.unpersist()
      e.name -> runs
    }

    println(f"\n${"expérience"}%-22s ${"Acc"}%14s ${"Rec_o"}%14s ${"Rec_d"}%14s ${"AUC"}%16s")
    results.foreach { case (name, runs) =>
      def ms(i: Int, k: Double): String = {
        val xs = runs.map(_(i) * k); val mu = xs.sum / xs.size
        val sd = if (xs.size > 1) math.sqrt(xs.map(x => (x - mu) * (x - mu)).sum / (xs.size - 1)) else 0.0
        if (k == 1) f"$mu%.4f ± $sd%.4f" else f"$mu%5.2f ± $sd%4.2f"
      }
      println(f"$name%-22s ${ms(0, 100)}%14s ${ms(1, 100)}%14s ${ms(2, 100)}%14s ${ms(3, 1)}%16s")
    }
    spark.stop()
  }
}
