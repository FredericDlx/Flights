// =============================================================================
// CONFIGURATION SCALA CLI & APACHE SPARK
// =============================================================================
// 1. Scala 2.13.15 est utilisé car Apache Spark 3.5.3 est compilé pour Scala 2.13
//    (les bibliothèques Spark SQL et Spark MLlib ne sont pas publiées en Scala 3).
// 2. Sur Java 17+, le système de modules JVM bloque par défaut l'accès par réflexion
//    interne qu'utilise Spark (Unsafe, DirectByteBuffer, Kryo, etc.).
//    Les options `--add-opens` ci-dessous débloquent ces accès automatiquement.
//> using scala 2.13.15
//> using dep org.apache.spark::spark-sql:3.5.3
//> using dep org.apache.spark::spark-mllib:3.5.3
//> using javaOpt -Xmx6g
//> using javaOpt --add-opens=java.base/java.lang=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.io=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.net=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.nio=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.security.action=ALL-UNNAMED
//> using javaOpt --add-opens=java.base/sun.util.calendar=ALL-UNNAMED

import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Projet Flight Delay Prediction — Reproduction et extension de l'article :
 * "Using Scalable Data Mining for Predicting Flight Delays"
 * (L. Belcastro, F. Marozzo, D. Talia, P. Trunfio, ACM TIST, 2016).
 *
 * Ce programme met en oeuvre l'intégralité du processus décrit dans la Figure 2 de l'article :
 *   1. Chargement des 3 sources de données (Vols AOTP, Météo QCLCD, Fuseaux horaires / WBAN)
 *   2. Prétraitement et nettoyage des données :
 *      - Table des observations météo (OT - Définition 2.2 & Section 4.1)
 *      - Table des vols (FT - Définition 2.1 & Section 4.1)
 *   3. Transformation et jointure spatio-temporelle en deux étapes (Table JT - Section 4.1 & Algorithme 1) :
 *      - Étape 1 : Jointure avec la météo de l'aéroport d'origine sur [tsd - 12h, tsd] -> vecteur Wo
 *      - Étape 2 : Jointure avec la météo de l'aéroport de destination sur [tsa - 12h, tsa] -> vecteur Wd
 *   4. Création des ensembles de données cibles D1, D2, D3, D4 (Section 4.2 & Tableau VI)
 *      et équilibrage des classes par sous-échantillonnage aléatoire (Figure 4)
 *   5. Extraction des caractéristiques (features) et Modélisation parallèle avec Spark ML (Section 4.3) :
 *      - DecisionTreeClassifier (demandé dans les consignes du projet)
 *      - RandomForestClassifier (modèle retenu dans l'article)
 *      - GBTClassifier (Gradient-Boosted Trees, modèle supplémentaire pour comparaison)
 *   6. Évaluation selon la matrice de confusion de l'article (Section 2.4, Tableau II, Équations 1 & 2) :
 *      - Précision globale (Acc), Rappel à l'heure (Rec_o), Rappel en retard (Rec_d) et AUC-ROC.
 */
object FlightApp {

  /**
   * Structure représentant une observation météorologique aéroportuaire O (Définition 2.2 de l'article) :
   * O = ⟨A, t, T, H, Wd, Ws, P, S, V, D⟩
   *
   * @param obsEpochSec   t  : Heure de l'observation (en secondes Unix pour accélérer la recherche dichotomique)
   * @param temperature   T  : Température en degrés Celsius (DryBulbCelsius)
   * @param humidity      H  : Humidité relative en % (RelativeHumidity)
   * @param windDirection Wd : Direction du vent en degrés (WindDirection)
   * @param windSpeed     Ws : Vitesse du vent (WindSpeed)
   * @param pressure      P  : Pression barométrique à la station (StationPressure)
   * @param sky           S  : État du ciel au format METAR (SkyCondition, ex. CLR, FEW, SCT, BKN, OVC)
   * @param visibility    V  : Visibilité horizontale (Visibility)
   * @param weatherDesc   D  : Descripteur des phénomènes météo au format METAR (WeatherType, ex. +SN, TSGR, RA)
   */
  case class WeatherObs(
    obsEpochSec: Long,
    temperature: Double,
    humidity: Double,
    windDirection: Double,
    windSpeed: Double,
    pressure: Double,
    sky: String,
    visibility: Double,
    weatherDesc: String
  )

  /**
   * Structure stockant les résultats d'évaluation d'un classificateur (Section 2.4, Tableau II) :
   * - TP (Vrai Positif)  : vol réellement à l'heure (0.0) et prédit à l'heure (0.0)
   * - FN (Faux Négatif)  : vol réellement à l'heure (0.0) mais prédit en retard (1.0)
   * - FP (Faux Positif)  : vol réellement en retard (1.0) mais prédit à l'heure (0.0)
   * - TN (Vrai Négatif)  : vol réellement en retard (1.0) et prédit en retard (1.0)
   * - accuracy (Acc)     : (TP + TN) / (TP + TN + FP + FN)  [Équation 1]
   * - recallOnTime (Reco): TP / (TP + FN)                   [Équation 2]
   * - recallDelayed(Recd): TN / (TN + FP)                   [Équation 2]
   * - aucRoc             : Aire sous la courbe ROC (Figures 7-b, 8-b, 9-b)
   */
  case class EvaluationMetrics(
    modelName: String,
    tp: Long,
    tn: Long,
    fp: Long,
    fn: Long,
    accuracy: Double,
    recallOnTime: Double,
    recallDelayed: Double,
    aucRoc: Double
  )

  def main(args: Array[String]): Unit = {
    // Initialisation de la session Spark en mode local sur tous les coeurs disponibles ("local[*]")
    // Pour exécuter sur un cluster (YARN / Standalone / Databricks), il suffit de retirer .master("local[*]")
    val spark = SparkSession.builder()
      .appName("Flight Delay Prediction")
      .master("local[*]")
      .getOrCreate()

    // Réduction de la verbosité des logs Spark pour ne garder que les avertissements et nos affichages
    spark.sparkContext.setLogLevel("WARN")

    val baseDir = "flights_data"

    // =========================================================================
    // ÉTAPE 1 : Chargement des fichiers bruts
    // =========================================================================
    // Astuce méthodologique (cf. Flight-project-indications.pdf) :
    // - En local : on teste d'abord sur 1 mois ("201201.csv" et "201201hourly.txt") pour valider rapidement.
    // - Sur cluster (ou pour l'analyse complète) : remplacer par "*.csv" et "*hourly.txt" pour charger
    //   tous les fichiers d'un seul coup avec Spark.
    val timezoneDf   = loadTimezones(spark, s"$baseDir/wban_airport_timezone.csv")
    val rawFlightsDf = loadFlights(spark, s"$baseDir/data/Flights/*.csv")
    val rawWeatherDf = loadWeather(spark, s"$baseDir/data/Weather/*hourly.txt")

    println(s"[Étape 1] Aéroports/Fuseaux horaires chargés : ${timezoneDf.count()} lignes")
    println(s"[Étape 1] Vols bruts chargés (AOTP)          : ${rawFlightsDf.count()} lignes")
    println(s"[Étape 1] Relevés météo bruts chargés (QCLCD): ${rawWeatherDf.count()} lignes")

    // =========================================================================
    // ÉTAPE 2 : Prétraitement des observations météo -> Table OT (Section 4.1)
    // =========================================================================
    val otDf = preprocessWeather(rawWeatherDf, timezoneDf)
    printWeatherFilteringDetails(rawWeatherDf, timezoneDf)
    println("=== Aperçu de la table des observations météo retenues (OT) ===")
    otDf.show(5, truncate = false)

    // =========================================================================
    // ÉTAPE 3 : Prétraitement des vols -> Table FT (Section 4.1)
    // =========================================================================
    val ftDf = preprocessFlights(rawFlightsDf, timezoneDf)
    printFlightsFilteringDetails(rawFlightsDf, timezoneDf)
    println("=== Aperçu de la table des vols retenus (FT) ===")
    ftDf.show(5, truncate = false)

    // =========================================================================
    // ÉTAPE 4 : Jointure spatio-temporelle en 2 étapes -> Table JT (Section 4.1 & Algo. 1)
    // =========================================================================
    // IMPORTANT : On ne met PAS `jtDf` en `.cache()` sur l'ensemble des 36 fichiers, car `jtDf`
    // contient des millions de vols avec 26 structures météo chacun (> 10 Go), ce qui saturerait
    // la RAM et l'espace disque `/tmp` ! C'est `dailyWeatherDf` (dans `joinFlightsAndWeather`)
    // et les ensembles finaux équilibrés `trainDf` / `testDf` (~30 Mo) qui sont mis en cache.
    val jtDf = joinFlightsAndWeather(ftDf, otDf, numHours = 12)
    println("=== Table jointe (JT : {F, Wo, Wd}) ===")
    val jtCount = jtDf.count()
    println(s"[Étape 4] Vols joints avec météo (JT)        : $jtCount lignes")
    jtDf.select("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "tsd", "tsa", "Wo", "Wd")
      .show(3, truncate = true)

    // =========================================================================
    // ÉTAPE 5 : Création du jeu cible (D1..D4) et équilibrage 50/50 (Section 4.2)
    // =========================================================================
    // Paramètres d'évaluation (cf. Tableau VII de l'article, page 14) :
    // - thresholdMinutes : Seuil de retard Th en minutes (valeurs de l'article : 15, 30, 45, 60, 90)
    // - targetDatasetId  : Jeu de données cible ("D1", "D2", "D3" ou "D4")
    // - mOriginObs       : Nombre d'observations météo horaires prises au départ (0, 1, 3, 5, 7, 9, 11)
    // - nDestObs         : Nombre d'observations météo horaires prises à l'arrivée (0, 1, 3, 5, 7, 9, 11)
    // Dans l'article (Section 5, Figure 7-a), m = 7 et n = 7 sur D2 avec Th = 60 min donne
    // le meilleur compromis performance/coût (Acc = 85.8%, Rec_o = 84.7%, Rec_d = 86.9%).
    val thresholdMinutes = 60.0
    val targetDatasetId  = "D2"
    val mOriginObs       = 7
    val nDestObs         = 7

    val targetDf = createTargetDataset(jtDf, datasetId = targetDatasetId, threshold = thresholdMinutes)
    // On équilibre d'abord les classes (50/50) avant d'extraire les vecteurs de caractéristiques
    // et de mettre en cache uniquement `trainDf` et `testDf` (beaucoup plus compacts en mémoire).
    val (balancedTrainDf, balancedTestDf) = splitAndBalance(targetDf, seed = 42L)
    val trainDf = extractFeatures(balancedTrainDf, mOrigin = mOriginObs, nDest = nDestObs).cache()
    val testDf  = extractFeatures(balancedTestDf,  mOrigin = mOriginObs, nDest = nDestObs).cache()

    println(s"=== Jeu cible $targetDatasetId (Seuil = $thresholdMinutes min, m = $mOriginObs, n = $nDestObs) ===")
    println(s"[Étape 5] Nombre d'instances Train (50/50)   : ${trainDf.count()} lignes")
    println(s"[Étape 5] Nombre d'instances Test  (50/50)   : ${testDf.count()} lignes")

    // =========================================================================
    // ÉTAPE 6 : Modélisation et Évaluation avec Spark ML (Sections 4.3 & 5)
    // =========================================================================
    val dtMetrics  = trainAndEvaluateDecisionTree(trainDf, testDf)
    val rfMetrics  = trainAndEvaluateRandomForest(trainDf, testDf)
    val gbtMetrics = trainAndEvaluateGBT(trainDf, testDf)

    printEvaluationSummary(Seq(dtMetrics, rfMetrics, gbtMetrics))

    spark.stop()
  }

  // ===========================================================================
  // FONCTIONS DE CHARGEMENT DES DONNÉES
  // ===========================================================================

  /**
   * 1. Chargement de la table de correspondance `wban_airport_timezone.csv`
   *
   * Difficulté résolue (importante à mentionner dans le rapport) :
   * - Les fichiers de vols utilisent `ORIGIN_AIRPORT_ID` et `DEST_AIRPORT_ID` (ex. 12266),
   *   tandis que les fichiers météo utilisent le code de station météo `WBAN` (ex. "03011" ou 3856).
   * - Dans les fichiers météo, `WBAN` est parfois préfixé par un zéro ("03011"), alors que dans
   *   `wban_airport_timezone.csv` il est écrit sous forme d'entier sans zéro initial (3856).
   *   Caster explicitement `WBAN` en `IntegerType` des deux côtés garantit que "03856" == 3856.
   * - De plus, cette table fournit le fuseau horaire `TimeZone` (ex. -5, -6, -8) indispensable
   *   pour calculer l'heure locale d'arrivée prévue `tsa`.
   */
  def loadTimezones(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("AirportID", col("AirportID").cast(IntegerType))
      .withColumn("WBAN", col("WBAN").cast(IntegerType))
      .withColumn("TimeZone", col("TimeZone").cast(IntegerType))
  }

  /**
   * 2. Chargement des fichiers CSV de vols (AOTP - Airline On-Time Performance)
   *
   * Difficulté résolue :
   * - Chaque ligne des fichiers CSV de vols se termine par une virgule finale (trailing comma) :
   *   `"FL_DATE",...,"WEATHER_DELAY","NAS_DELAY",`
   * - Spark interprète cette virgule finale comme une 13ème colonne vide nommée `_c12`.
   *   On la supprime immédiatement avec `.drop("_c12")`.
   */
  def loadFlights(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .drop("_c12")
  }

  /**
   * 3. Chargement des fichiers horaires de météo (QCLCD - Quality Controlled Local Climatological Data)
   * Bien que portant l'extension `.txt` (`201201hourly.txt`), ces fichiers sont structurés en CSV avec en-tête.
   */
  def loadWeather(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  // ===========================================================================
  // FONCTIONS DE PRÉTRAITEMENT (SECTION 4.1 DE L'ARTICLE)
  // ===========================================================================

  /**
   * Prétraitement des observations météo -> Table OT (Section 4.1 & Définition 2.2)
   *
   * Opérations effectuées :
   * 1. Filtrage des stations : comme indiqué à la Section 4.1 ("nous avons supprimé toutes les
   *    observations météorologiques non liées à des aéroports"), on effectue une jointure interne
   *    avec la liste des `WBAN` d'aéroports connus. L'utilisation de `broadcast(airportWbans)`
   *    évite un shuffle réseau coûteux sur les millions de lignes météo (Map-Side Join).
   * 2. Reconstruction du Timestamp :
   *    - Dans le fichier QCLCD, `Date` est un entier/texte au format `yyyyMMdd` (ex. 20120101)
   *      et `Time` est au format `HHmm` (ex. `15` ou `"0015"` pour 00h15).
   *    - Si `inferSchema` lit `Time` comme un entier, `"0015"` devient `15`. On utilise donc
   *      `lpad(..., 4, "0")` pour restaurer systématiquement les 4 chiffres `"0015"` avant
   *      de parser la date complète avec `to_timestamp(..., "yyyyMMdd HHmm")`.
   * 3. Nettoyage des valeurs manquantes ou spéciales :
   *    - Les données QCLCD utilisent `"M"` (Missing) pour les mesures absentes, `"VRB"` (Variable)
   *      pour la direction du vent, et contiennent parfois des espaces (`" 24"`).
   *    - La fonction `cleanDouble` supprime les espaces, remplace `"M"` par vide, et convertit en `Double`
   *      (les valeurs non numériques comme `"VRB"` deviennent `null` puis sont remplacées par `0.0`).
   */
  def preprocessWeather(rawWeatherDf: DataFrame, timezoneDf: DataFrame): DataFrame = {
    val airportWbans = timezoneDf.select("WBAN").distinct()

    // Fonction utilitaire pour extraire un nombre flottant propre depuis une colonne QCLCD :
    // - Dans les fichiers QCLCD de la NOAA, certaines valeurs numériques sont suivies d'un indicateur
    //   de qualité (ex. "29.92s" ou "10.00s" pour "suspect", ou précédées d'espaces " 24"),
    //   tandis que les valeurs manquantes/spéciales sont codées "M" (Missing) ou "VRB" (Variable).
    // - `regexp_extract` extrait uniquement la partie numérique signée (ex. "29.92s" -> "29.92"),
    //   et renvoie une chaîne vide "" pour "M" ou "VRB" (qui devient `null` après `.cast(DoubleType)`).
    def cleanDouble(c: Column): Column =
      nullif(regexp_extract(c.cast(StringType), "([-+]?[0-9]*\\.?[0-9]+)", 1), lit("")).cast(DoubleType)

    rawWeatherDf
      .withColumn("WBAN", col("WBAN").cast(IntegerType))
      // Suppression des observations météo qui ne correspondent pas à un aéroport (via Broadcast Join)
      .join(broadcast(airportWbans), Seq("WBAN"), "inner")
      // Formatage de Date (yyyyMMdd sur 8 chiffres) et Time (HHmm sur 4 chiffres) en Timestamp
      .withColumn("date_str", lpad(col("Date").cast(StringType), 8, "0"))
      .withColumn("time_str", lpad(col("Time").cast(StringType), 4, "0"))
      .withColumn(
        "obs_timestamp",
        to_timestamp(concat_ws(" ", col("date_str"), col("time_str")), "yyyyMMdd HHmm")
      )
      .filter(col("obs_timestamp").isNotNull)
      // Sélection des 10 champs du tuple O = ⟨A, t, T, H, Wd, Ws, P, S, V, D⟩ (Définition 2.2)
      .select(
        col("WBAN"),
        col("obs_timestamp"),
        cleanDouble(col("DryBulbCelsius")).as("temperature"),              // T  : Température (°C)
        cleanDouble(col("RelativeHumidity")).as("humidity"),               // H  : Humidité (%)
        cleanDouble(col("WindDirection")).as("wind_direction"),            // Wd : Direction du vent
        cleanDouble(col("WindSpeed")).as("wind_speed"),                    // Ws : Vitesse du vent
        cleanDouble(col("StationPressure")).as("pressure"),                // P  : Pression barométrique
        coalesce(trim(col("SkyCondition")), lit("CLR")).as("sky"),         // S  : État du ciel
        cleanDouble(col("Visibility")).as("visibility"),                   // V  : Visibilité
        coalesce(trim(col("WeatherType")), lit("NONE")).as("weather_desc") // D  : Phénomène météo
      )
      // Imputation des valeurs numériques manquantes ("M") par des valeurs physiques standard/neutres
      // (et NON par 0.0 partout, car visibilité = 0.0 signifie brouillard total et pression = 0.0 signifie tempête extrême !)
      .na.fill(Map(
        "temperature"    -> 15.0,  // Température standard ISA (15 °C)
        "humidity"       -> 50.0,  // Humidité relative moyenne (50 %)
        "wind_direction" -> 0.0,   // Vent calme ou variable ("VRB")
        "wind_speed"     -> 0.0,   // Vent nul
        "pressure"       -> 29.92, // Pression atmosphérique normale (29.92 inHg)
        "visibility"     -> 10.0   // Visibilité normale dégagée (10 miles dans QCLCD)
      ))
  }

  /**
   * Affiche le détail complet des observations météo retenues vs non retenues (Section 4.1) :
   * - Calcule en une seule passe le nombre de lignes rejetées par cause :
   *   1. WBAN nul ou non numérique
   *   2. Station WBAN non liée à un aéroport commercial de `wban_airport_timezone.csv`
   *   3. Date ou heure (`Date`/`Time`) invalide ne permettant pas de construire `obs_timestamp`
   * - Affiche également un échantillon de 5 observations météo non retenues.
   */
  def printWeatherFilteringDetails(rawWeatherDf: DataFrame, timezoneDf: DataFrame): Unit = {
    val airportWbans = timezoneDf.select(col("WBAN").as("AIRPORT_WBAN")).distinct()

    val auditedWeather = rawWeatherDf
      .withColumn("wban_int", col("WBAN").cast(IntegerType))
      .join(broadcast(airportWbans), col("wban_int") === col("AIRPORT_WBAN"), "left_outer")
      .withColumn("date_str", lpad(col("Date").cast(StringType), 8, "0"))
      .withColumn("time_str", lpad(col("Time").cast(StringType), 4, "0"))
      .withColumn(
        "obs_timestamp",
        to_timestamp(concat_ws(" ", col("date_str"), col("time_str")), "yyyyMMdd HHmm")
      )

    val stats = auditedWeather.agg(
      count("*").as("total"),
      sum(when(col("wban_int").isNull, 1L).otherwise(0L)).as("invalid_wban"),
      sum(when(col("wban_int").isNotNull && col("AIRPORT_WBAN").isNull, 1L).otherwise(0L)).as("non_airport_wban"),
      sum(when(col("AIRPORT_WBAN").isNotNull && col("obs_timestamp").isNull, 1L).otherwise(0L)).as("invalid_timestamp"),
      sum(when(col("AIRPORT_WBAN").isNotNull && col("obs_timestamp").isNotNull, 1L).otherwise(0L)).as("retained")
    ).first()

    val total            = stats.getLong(0)
    val invalidWban      = if (stats.isNullAt(1)) 0L else stats.getLong(1)
    val nonAirportWban   = if (stats.isNullAt(2)) 0L else stats.getLong(2)
    val invalidTimestamp = if (stats.isNullAt(3)) 0L else stats.getLong(3)
    val retained         = if (stats.isNullAt(4)) 0L else stats.getLong(4)
    val totalRejected    = total - retained

    def pct(n: Long): Double = if (total > 0) n * 100.0 / total.toDouble else 0.0

    println("\n------------------------------------------------------------------------------------------")
    println("DÉTAIL DU FILTRAGE MÉTÉO (QCLCD -> Table OT)")
    println("------------------------------------------------------------------------------------------")
    println(f"Total relevés météo bruts (QCLCD)                  : $total%,12d (100.00 %%)")
    println(f"  -> Relevés RETENUS dans OT (aéroports valides)   : $retained%,12d (${pct(retained)}%6.2f %%)")
    println(f"  -> Relevés NON RETENUS (rejetés au total)        : $totalRejected%,12d (${pct(totalRejected)}%6.2f %%)")
    println(f"       1. Station WBAN hors aéroports commerciaux  : $nonAirportWban%,12d (${pct(nonAirportWban)}%6.2f %%)")
    println(f"       2. Code WBAN manquant ou non numérique      : $invalidWban%,12d (${pct(invalidWban)}%6.2f %%)")
    println(f"       3. Date ou Heure (Date/Time) invalide       : $invalidTimestamp%,12d (${pct(invalidTimestamp)}%6.2f %%)")
    println("------------------------------------------------------------------------------------------")
    println("Exemples d'observations météo NON RETENUES (stations hors aéroports / invalides) :")
    auditedWeather
      .filter(col("AIRPORT_WBAN").isNull || col("obs_timestamp").isNull)
      .select("WBAN", "Date", "Time", "DryBulbCelsius", "RelativeHumidity", "WindSpeed", "SkyCondition", "Visibility")
      .show(5, truncate = false)
  }

  /**
   * Prétraitement des vols -> Table FT (Section 4.1 & Définition 2.1)
   *
   * Opérations et difficultés résolues (essentiel pour le rapport) :
   * 1. Filtrage des vols annulés et déroutés :
   *    Conformément à la Section 4.1 ("notre étude portant uniquement sur les vols retardés,
   *    nous avons filtré les vols déroutés et annulés"), on garde `CANCELLED == 0.0` et `DIVERTED == 0.0`.
   * 2. Calcul de l'heure de départ prévue `tsd` et d'arrivée prévue `tsa` :
   *    - Dans la Définition 2.1, un vol possède une heure de départ prévue `tsd` (dans le fuseau
   *      horaire de l'aéroport d'origine Ao) et une heure d'arrivée prévue `tsa` (dans le fuseau
   *      horaire de l'aéroport de destination Ad).
   *    - Or, nos fichiers CSV de vols contiennent `FL_DATE`, `CRS_DEP_TIME` (heure de départ prévue)
   *      et `CRS_ELAPSED_TIME` (durée prévue du vol en minutes), mais PAS la colonne `CRS_ARR_TIME` !
   *    - De plus, `tsa` peut tomber le lendemain (vols de nuit) et dans un fuseau horaire différent.
   *    - C'est pourquoi on joint le vol avec `wban_airport_timezone.csv` pour récupérer les fuseaux
   *      horaires `ORIGIN_TZ` et `DEST_TZ`, puis on calcule exactement l'heure locale d'arrivée `tsa` :
   *        tsa = tsd + CRS_ELAPSED_TIME (en secondes) + (DEST_TZ - ORIGIN_TZ) * 3600
   *    - Cela garantit que `tsa` est exprimée dans l'heure locale de l'aéroport de destination Ad,
   *      ce qui est indispensable pour la joindre avec les relevés météo locaux de la station Ad !
   * 3. Cas particulier `CRS_DEP_TIME == 2400` :
   *    Certains vols ont une heure codée `"2400"`, que l'on normalise en `"0000"`.
   * 4. Valeurs manquantes de `WEATHER_DELAY` et `NAS_DELAY` :
   *    Dans AOTP, ces colonnes ne sont renseignées que si le vol a >= 15 min de retard (sinon elles sont `null`).
   *    On remplace les `null` par `0.0`.
   */
  def preprocessFlights(rawFlightsDf: DataFrame, timezoneDf: DataFrame): DataFrame = {
    val originTz = timezoneDf.select(
      col("AirportID").as("ORIGIN_AIRPORT_ID"),
      col("WBAN").as("ORIGIN_WBAN"),
      col("TimeZone").as("ORIGIN_TZ")
    )
    val destTz = timezoneDf.select(
      col("AirportID").as("DEST_AIRPORT_ID"),
      col("WBAN").as("DEST_WBAN"),
      col("TimeZone").as("DEST_TZ")
    )

    rawFlightsDf
      // 1. Suppression des vols annulés, déroutés ou incomplets
      .filter(col("CANCELLED") === 0.0 && col("DIVERTED") === 0.0)
      .filter(col("ARR_DELAY_NEW").isNotNull && col("CRS_ELAPSED_TIME").isNotNull && col("CRS_DEP_TIME").isNotNull)
      // 2. Association de l'aéroport d'origine et de destination avec leur WBAN et TimeZone
      .join(broadcast(originTz), Seq("ORIGIN_AIRPORT_ID"), "inner")
      .join(broadcast(destTz), Seq("DEST_AIRPORT_ID"), "inner")
      // 3. Normalisation de CRS_DEP_TIME sur 4 chiffres ("845" -> "0845", "2400" -> "0000")
      .withColumn("dep_time_str",
        when(lpad(col("CRS_DEP_TIME").cast(IntegerType).cast(StringType), 4, "0") === "2400", lit("0000"))
          .otherwise(lpad(col("CRS_DEP_TIME").cast(IntegerType).cast(StringType), 4, "0"))
      )
      // 4. Construction du timestamp local de départ prévu `tsd`
      .withColumn(
        "tsd",
        to_timestamp(concat_ws(" ", date_format(col("FL_DATE"), "yyyy-MM-dd"), col("dep_time_str")), "yyyy-MM-dd HHmm")
      )
      // 5. Calcul du timestamp local d'arrivée prévu `tsa` :
      //    tsa = tsd + durée_vol_prévue + décalage_fuseau_horaire(destination - origine)
      .withColumn(
        "tsa",
        from_unixtime(
          unix_timestamp(col("tsd")) +
          (col("CRS_ELAPSED_TIME").cast(LongType) * 60L) +
          ((col("DEST_TZ") - col("ORIGIN_TZ")).cast(LongType) * 3600L)
        ).cast(TimestampType)
      )
      // 6. Remplacement des valeurs nulles de retard météo/NAS par 0.0
      .na.fill(0.0, Seq("WEATHER_DELAY", "NAS_DELAY"))
      .select(
        col("FL_DATE"),
        col("OP_CARRIER_AIRLINE_ID"),
        col("OP_CARRIER_FL_NUM"),
        col("ORIGIN_AIRPORT_ID"),
        col("ORIGIN_WBAN"),
        col("DEST_AIRPORT_ID"),
        col("DEST_WBAN"),
        col("tsd"),
        col("tsa"),
        col("CRS_ELAPSED_TIME"),
        col("ARR_DELAY_NEW"),
        col("WEATHER_DELAY"),
        col("NAS_DELAY")
      )
  }

  /**
   * Affiche le détail complet des vols retenus vs non retenus (Section 3, Tableau III & Section 4.1) :
   * - Calcule en une seule passe la répartition exacte des vols rejetés :
   *   1. Vols annulés (`CANCELLED == 1.0`)
   *   2. Vols déroutés (`DIVERTED == 1.0` et non annulés)
   *   3. Vols avec horaires ou retard manquants (`ARR_DELAY_NEW`, `CRS_ELAPSED_TIME` ou `CRS_DEP_TIME` nul)
   *   4. Vols dont l'aéroport d'origine ou de destination est absent de `wban_airport_timezone.csv`
   * - Affiche également un échantillon de 5 vols non retenus avec la raison de leur rejet.
   */
  def printFlightsFilteringDetails(rawFlightsDf: DataFrame, timezoneDf: DataFrame): Unit = {
    val originIds = timezoneDf.select(col("AirportID").as("VALID_ORIGIN_ID")).distinct()
    val destIds   = timezoneDf.select(col("AirportID").as("VALID_DEST_ID")).distinct()

    val auditedFlights = rawFlightsDf
      .join(broadcast(originIds), col("ORIGIN_AIRPORT_ID") === col("VALID_ORIGIN_ID"), "left_outer")
      .join(broadcast(destIds),   col("DEST_AIRPORT_ID")   === col("VALID_DEST_ID"),   "left_outer")

    val isCancelled = col("CANCELLED") === 1.0
    val isDiverted  = !isCancelled && (col("DIVERTED") === 1.0)
    val isMissingFields = !isCancelled && !isDiverted &&
      (col("ARR_DELAY_NEW").isNull || col("CRS_ELAPSED_TIME").isNull || col("CRS_DEP_TIME").isNull)
    val isUnknownAirport = !isCancelled && !isDiverted && !isMissingFields &&
      (col("VALID_ORIGIN_ID").isNull || col("VALID_DEST_ID").isNull)
    val isRetained = !isCancelled && !isDiverted && !isMissingFields && !isUnknownAirport

    val stats = auditedFlights.agg(
      count("*").as("total"),
      sum(when(isCancelled, 1L).otherwise(0L)).as("cancelled"),
      sum(when(isDiverted, 1L).otherwise(0L)).as("diverted"),
      sum(when(isMissingFields, 1L).otherwise(0L)).as("missing_fields"),
      sum(when(isUnknownAirport, 1L).otherwise(0L)).as("unknown_airport"),
      sum(when(isRetained, 1L).otherwise(0L)).as("retained")
    ).first()

    val total          = stats.getLong(0)
    val cancelled      = if (stats.isNullAt(1)) 0L else stats.getLong(1)
    val diverted       = if (stats.isNullAt(2)) 0L else stats.getLong(2)
    val missingFields  = if (stats.isNullAt(3)) 0L else stats.getLong(3)
    val unknownAirport = if (stats.isNullAt(4)) 0L else stats.getLong(4)
    val retained       = if (stats.isNullAt(5)) 0L else stats.getLong(5)
    val totalRejected  = total - retained

    def pct(n: Long): Double = if (total > 0) n * 100.0 / total.toDouble else 0.0

    println("\n------------------------------------------------------------------------------------------")
    println("DÉTAIL DU FILTRAGE DES VOLS (AOTP -> Table FT, cf. Tableau III de l'article)")
    println("------------------------------------------------------------------------------------------")
    println(f"Total vols bruts chargés (AOTP)                    : $total%,12d (100.00 %%)")
    println(f"  -> Vols RETENUS dans FT                          : $retained%,12d (${pct(retained)}%6.2f %%)")
    println(f"  -> Vols NON RETENUS (rejetés au total)           : $totalRejected%,12d (${pct(totalRejected)}%6.2f %%)")
    println(f"       1. Vols annulés (CANCELLED == 1)            : $cancelled%,12d (${pct(cancelled)}%6.2f %%)")
    println(f"       2. Vols déroutés (DIVERTED == 1)            : $diverted%,12d (${pct(diverted)}%6.2f %%)")
    println(f"       3. Horaires ou retard manquants (NULL)      : $missingFields%,12d (${pct(missingFields)}%6.2f %%)")
    println(f"       4. Aéroport hors table WBAN/TimeZone        : $unknownAirport%,12d (${pct(unknownAirport)}%6.2f %%)")
    println("------------------------------------------------------------------------------------------")
    println("Exemples de vols NON RETENUS (avec le motif de rejet) :")
    auditedFlights
      .filter(!isRetained)
      .withColumn(
        "motif_rejet",
        when(isCancelled, lit("Annulé (CANCELLED=1)"))
          .when(isDiverted, lit("Dérouté (DIVERTED=1)"))
          .when(isMissingFields, lit("Valeur manquante (NULL)"))
          .otherwise(lit("Aéroport sans WBAN"))
      )
      .select("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "ARR_DELAY_NEW", "CANCELLED", "DIVERTED", "motif_rejet")
      .show(5, truncate = false)
  }

  // ===========================================================================
  // JOINTURE SPATIO-TEMPORELLE EN 2 ÉTAPES (SECTION 4.1, FIGURE 3 & ALGORITHME 1)
  // ===========================================================================

  /**
   * Jointure en deux étapes entre la table des vols (FT) et la table météo (OT) pour produire JT.
   *
   * Objectif (Section 4.1) :
   * Pour chaque vol F, construire le tuple {F, Wo, Wd, C} où :
   * - Wo = ⟨O(Ao, tsd), O(Ao, tsd - 1h), ..., O(Ao, tsd - 12h)⟩ (13 observations horaires au départ)
   * - Wd = ⟨O(Ad, tsa), O(Ad, tsa - 1h), ..., O(Ad, tsa - 12h)⟩ (13 observations horaires à l'arrivée)
   *
   * Pourquoi cette jointure est complexe et comment nous l'avons optimisée (à détailler dans le rapport) :
   * 1. Problème du produit cartésien / asymétrie (Data Skew) :
   *    Une jointure SQL naïve sur `FT.WBAN == OT.WBAN AND OT.time BETWEEN tsd - 12h AND tsd`
   *    repartitionnerait toutes les données uniquement par `WBAN` (~300 stations). Les grands hubs
   *    (Atlanta, Chicago, Dallas) généreraient des milliards de paires (vols × observations sur 5 ans)
   *    dans une seule partition, provoquant un OutOfMemoryError.
   * 2. Solution de l'Algorithme 1 (Jointure par répartition sur clé composite ⟨A, Date⟩) :
   *    En prenant le couple `⟨WBAN, Date⟩` comme clé de jointure, on divise le problème par station ET par jour.
   *    Pour un couple `⟨WBAN, Date⟩` donné, il n'y a qu'environ 24 à 48 observations météo !
   *    On peut donc regrouper et trier chronologiquement ces observations dans un petit tableau en mémoire `AO`
   *    (exactement comme dans la fonction `Reduce` de l'Algorithme 1).
   * 3. Problème du passage de minuit (et correction d'une coquille de l'Algorithme 1 de l'article) :
   *    - Un vol qui décolle le jour D à 05h40 (ex. `F(ATL, IAD, 02/12/2014 05h40)` dans la Figure 3)
   *      a besoin de la météo jusqu'à 12h avant, soit le jour D-1 jusqu'à 17h40 !
   *    - Dans le pseudo-code de l'Algorithme 1 de l'article (page 11, lignes 11-15), les auteurs ont écrit :
   *      `if Date(V.tsd).plusHours(12) is Date(V.tsd).plusDays(1) then join_key <- <V.Ao, Date(V.tsd).plusDays(1)>`
   *      dans la branche `else` (celle des vols `FT`). Or, appliquée à `FT`, cette condition envoie un vol
   *      de l'après-midi du jour D vers le réducteur du jour D+1 (au lieu de récupérer la météo de D-1),
   *      et de surcroît émet le vol deux fois !
   *    - La bonne logique consiste à appliquer cette duplication aux observations météo `OT` de l'après-midi :
   *      toute observation météo du jour D-1 ayant lieu entre 12h00 et 23h59 (`hour >= 12`, c'est-à-dire
   *      telle que `Date(O.t + 12h) == Date(O.t) + 1 jour`) est envoyée à la fois au jour `D-1` et au jour `D`.
   *      Ainsi, le tableau `AO` du jour `D` contient toutes les observations de `[D-1 12h00 .. D 23h59]`,
   *      et chaque vol du jour `D` (qu'il parte le matin ou le soir) trouve ses 12 heures précédentes !
   * 4. Recherche de l'observation la plus proche (`get_hourly_observations`) :
   *    Les relevés météo n'étant pas pris pile à `:00` (ex. `00:15`, `00:53`), pour chaque décalage
   *    horaire `h` de `0` à `12`, on recherche par dichotomie ($O(\log K)$) dans le tableau trié `AO`
   *    l'observation dont le timestamp est le plus proche de `t - h heures`.
   */
  def joinFlightsAndWeather(ftDf: DataFrame, otDf: DataFrame, numHours: Int = 12): DataFrame = {
    val obsWithEpoch = otDf
      .withColumn("obs_epoch", unix_timestamp(col("obs_timestamp")))
      .withColumn("obs_date", to_date(col("obs_timestamp")))
      // Le premier champ de la struct est `obsEpochSec` : ainsi `sort_array` triera par ordre chronologique !
      .withColumn(
        "obs_struct",
        struct(
          col("obs_epoch").as("obsEpochSec"),
          col("temperature"),
          col("humidity"),
          col("wind_direction").as("windDirection"),
          col("wind_speed").as("windSpeed"),
          col("pressure"),
          col("sky"),
          col("visibility"),
          col("weather_desc").as("weatherDesc")
        )
      )

    // 1. Observations météo associées à leur propre jour J : clé ⟨WBAN, J⟩
    val sameDayObs = obsWithEpoch.select(
      col("WBAN"),
      col("obs_date").as("join_date"),
      col("obs_struct")
    )

    // 2. Observations météo de la veille (J-1 après 12h00) également associées au jour J : clé ⟨WBAN, J⟩
    //    Permet aux vols ayant lieu entre 00h00 et 12h00 le jour J d'avoir accès aux 12 heures précédentes.
    val prevDayAfternoonObs = obsWithEpoch
      .filter(hour(col("obs_timestamp")) >= (24 - numHours))
      .select(
        col("WBAN"),
        date_add(col("obs_date"), 1).as("join_date"),
        col("obs_struct")
      )

    // 3. Création du tableau trié AO par ⟨WBAN, join_date⟩ (équivalent de la boucle sur OT dans Reduce, Algo. 1)
    //    On met `dailyWeatherDf` en cache car il est compact et utilisé deux fois (Origine + Destination).
    val dailyWeatherDf = sameDayObs
      .union(prevDayAfternoonObs)
      .groupBy("WBAN", "join_date")
      .agg(sort_array(collect_list("obs_struct"), asc = true).as("AO"))
      .cache()

    // 4. UDF `get_hourly_observations(AO, flightEpochSec)` (ligne 27 de l'Algorithme 1) :
    //    Construit le tableau de (numHours + 1) observations : [O(t), O(t - 1h), ..., O(t - numHours)]
    val getHourlyObservationsUdf: UserDefinedFunction = udf((ao: Seq[Row], flightEpochSec: Long) => {
      if (ao == null || ao.isEmpty) {
        Seq.empty[WeatherObs]
      } else {
        val epochs = ao.map(_.getLong(0)).toArray
        (0 to numHours).map { h =>
          val targetSec = flightEpochSec - (h * 3600L)
          val idx = findClosestIndex(epochs, targetSec)
          val r = ao(idx)
          WeatherObs(
            obsEpochSec   = r.getLong(0),
            temperature   = r.getDouble(1),
            humidity      = r.getDouble(2),
            windDirection = r.getDouble(3),
            windSpeed     = r.getDouble(4),
            pressure      = r.getDouble(5),
            sky           = r.getString(6),
            visibility    = r.getDouble(7),
            weatherDesc   = r.getString(8)
          )
        }
      }
    })

    // --- Première étape de jointure : Météo à l'aéroport d'origine (Wo) ---
    val originWeatherDf = dailyWeatherDf
      .withColumnRenamed("WBAN", "ORIGIN_WBAN")
      .withColumnRenamed("join_date", "origin_date")
      .withColumnRenamed("AO", "AO_origin")

    val step1Df = ftDf
      .withColumn("origin_date", to_date(col("tsd")))
      .join(originWeatherDf, Seq("ORIGIN_WBAN", "origin_date"), "inner")
      .withColumn("Wo", getHourlyObservationsUdf(col("AO_origin"), unix_timestamp(col("tsd"))))
      .drop("origin_date", "AO_origin")

    // --- Deuxième étape de jointure : Météo à l'aéroport de destination (Wd) ---
    val destWeatherDf = dailyWeatherDf
      .withColumnRenamed("WBAN", "DEST_WBAN")
      .withColumnRenamed("join_date", "dest_date")
      .withColumnRenamed("AO", "AO_dest")

    val step2Df = step1Df
      .withColumn("dest_date", to_date(col("tsa")))
      .join(destWeatherDf, Seq("DEST_WBAN", "dest_date"), "inner")
      .withColumn("Wd", getHourlyObservationsUdf(col("AO_dest"), unix_timestamp(col("tsa"))))
      .drop("dest_date", "AO_dest")

    step2Df
  }

  /**
   * Recherche par dichotomie (Binary Search en O(log K)) dans le tableau trié `sortedEpochs`
   * de l'indice dont la valeur est la plus proche de l'heure recherchée `target`.
   */
  private def findClosestIndex(sortedEpochs: Array[Long], target: Long): Int = {
    var low = 0
    var high = sortedEpochs.length - 1
    while (low <= high) {
      val mid = (low + high) >>> 1
      val midVal = sortedEpochs(mid)
      if (midVal < target) low = mid + 1
      else if (midVal > target) high = mid - 1
      else return mid
    }
    if (low >= sortedEpochs.length) sortedEpochs.length - 1
    else if (high < 0) 0
    else {
      if (math.abs(sortedEpochs(low) - target) < math.abs(sortedEpochs(high) - target)) low else high
    }
  }

  // ===========================================================================
  // CRÉATION DES DONNÉES CIBLES D1..D4 & ÉQUILIBRAGE (SECTION 4.2 DE L'ARTICLE)
  // ===========================================================================

  /**
   * Création d'un ensemble de données cible D1, D2, D3 ou D4 (Section 4.2, Tableau VI) :
   *
   * Pourquoi filtrer les causes de retard (Section 3 & Section 4.2) ?
   * Un vol peut être retardé pour 5 causes : Compagnie (Carrier), Arrivée tardive de l'avion (Late Aircraft),
   * Système national de l'espace aérien (NAS), Météo extrême (Extreme Weather), et Sécurité.
   * Notre modèle n'ayant en entrée que les horaires et la météo aux aéroports de départ et d'arrivée,
   * il cherche à prédire les retards liés à la météo (Extreme Weather et NAS).
   * Les auteurs définissent donc 4 jeux de données cibles (avec la relation (D1d U D2d) c D3d c D4d) :
   * - D1 : Vols retardés UNIQUEMENT par la météo extrême, le NAS, ou une combinaison des deux
   *        (c.-à-d. `WEATHER_DELAY + NAS_DELAY >= ARR_DELAY_NEW`).
   * - D2 : Vols retardés par la météo extrême (`WEATHER_DELAY > 0`) OU dont le retard NAS atteint au moins
   *        le seuil Th (`NAS_DELAY >= threshold`). C'est le jeu de données principal utilisé en Section 5.
   * - D3 : Vols retardés ayant au moins une composante météo extrême ou NAS (`WEATHER_DELAY > 0` ou `NAS_DELAY > 0`),
   *        même si d'autres causes (compagnie, avion en retard) y contribuent aussi.
   * - D4 : Tous les vols dont le retard total à l'arrivée `ARR_DELAY_NEW >= threshold`, quelle qu'en soit la cause.
   *
   * Attribut de classe C (`label`) selon les Définitions 2.4 et 2.5 :
   * - `label = 0.0` : Vol à l'heure (`ARR_DELAY_NEW < threshold`)
   * - `label = 1.0` : Vol en retard (`ARR_DELAY_NEW >= threshold` et vérifiant la condition de Di)
   */
  def createTargetDataset(jtDf: DataFrame, datasetId: String, threshold: Double): DataFrame = {
    val isDelayed = col("ARR_DELAY_NEW") >= threshold

    val weatherDelayedCondition: Column = datasetId.toUpperCase match {
      case "D1" =>
        isDelayed &&
        (col("WEATHER_DELAY") > 0.0 || col("NAS_DELAY") > 0.0) &&
        ((col("WEATHER_DELAY") + col("NAS_DELAY")) >= col("ARR_DELAY_NEW"))

      case "D2" =>
        isDelayed && (col("WEATHER_DELAY") > 0.0 || col("NAS_DELAY") >= threshold)

      case "D3" =>
        isDelayed && (col("WEATHER_DELAY") > 0.0 || col("NAS_DELAY") > 0.0)

      case "D4" =>
        isDelayed

      case other =>
        throw new IllegalArgumentException(s"Dataset ID inconnu : $other (attendu : D1, D2, D3 ou D4)")
    }

    val isOnTime = col("ARR_DELAY_NEW") < threshold

    jtDf
      .filter(isOnTime || weatherDelayedCondition)
      .withColumn("label", when(isDelayed, 1.0).otherwise(0.0))
  }

  /**
   * Sous-échantillonnage aléatoire (Random Undersampling - Section 4.2 & Figure 4, page 12) :
   *
   * Pourquoi équilibrer les données ?
   * - Dans la réalité (Tableau III), ~80% des vols sont à l'heure et seulement ~19% sont en retard
   *   (et encore moins avec un seuil de 60 minutes ou un filtrage D1/D2).
   * - Si l'on entraînait un classificateur sur ce jeu déséquilibré, il prédirait presque toujours "à l'heure"
   *   et aurait un très mauvais rappel sur les vols en retard (Rec_d).
   * - Conformément à la Figure 4 de l'article :
   *   1. On sépare les tuples en deux groupes : "en retard" (`label == 1.0`) et "à l'heure" (`label == 0.0`).
   *   2. On tire aléatoirement sans remise autant de vols "à l'heure" qu'il y a de vols "en retard".
   *   3. On répartit chaque classe selon un rapport 3:1 (75% dans l'ensemble d'apprentissage, 25% dans le test).
   *   4. On obtient ainsi une paire (Train, Test) parfaitement équilibrée (50% à l'heure, 50% en retard).
   */
  def splitAndBalance(targetDf: DataFrame, seed: Long = 42L): (DataFrame, DataFrame) = {
    // Calcul en une seule passe du nombre de vols à l'heure (0.0) et en retard (1.0)
    val countsRow = targetDf.agg(
      sum(when(col("label") === 1.0, 1L).otherwise(0L)).as("delayed"),
      sum(when(col("label") === 0.0, 1L).otherwise(0L)).as("ontime")
    ).first()

    val delayedCount = if (countsRow.isNullAt(0)) 0L else countsRow.getLong(0)
    val onTimeCount  = if (countsRow.isNullAt(1)) 0L else countsRow.getLong(1)

    println(s"[Étape 5] Jeu cible avant équilibrage        : ${delayedCount + onTimeCount} lignes " +
      s"(À l'heure = $onTimeCount, En retard = $delayedCount)")

    val delayedDf = targetDf.filter(col("label") === 1.0)
    val onTimeDf  = targetDf.filter(col("label") === 0.0)

    if (delayedCount == 0 || onTimeCount == 0) {
      return (targetDf.limit(0), targetDf.limit(0))
    }

    // Fraction de la classe majoritaire ("à l'heure") à conserver pour égaler la classe minoritaire ("en retard")
    val ratio = math.min(1.0, delayedCount.toDouble / onTimeCount.toDouble)
    val sampledOnTimeDf = onTimeDf.sample(withReplacement = false, fraction = ratio, seed = seed)

    // Division 75% Train / 25% Test (rapport 3:1 de l'article, page 11)
    val Array(delayedTrain, delayedTest) = delayedDf.randomSplit(Array(0.75, 0.25), seed = seed)
    val Array(onTimeTrain, onTimeTest)   = sampledOnTimeDf.randomSplit(Array(0.75, 0.25), seed = seed)

    val trainDf = delayedTrain.union(onTimeTrain)
    val testDf  = delayedTest.union(onTimeTest)

    (trainDf, testDf)
  }

  // ===========================================================================
  // EXTRACTION DES CARACTÉRISTIQUES & MODÉLISATION SPARK ML (SECTIONS 4.3 & 5)
  // ===========================================================================

  /**
   * Construction du vecteur de caractéristiques `features` pour Spark ML (Section 4.3 & Section 5) :
   *
   * Le prédicteur Md,m,n (page 13 & page 14) prend en entrée :
   * 1. Les informations du vol programmé Fs (7 caractéristiques) :
   *    - ORIGIN_AIRPORT_ID : identifiant de l'aéroport de départ Ao
   *    - DEST_AIRPORT_ID   : identifiant de l'aéroport d'arrivée Ad
   *    - day_of_month      : jour du mois (1..31)
   *    - day_of_week       : jour de la semaine (1..7, car les retards varient selon le jour, cf. p. 16)
   *    - dep_hour          : tranche horaire de départ prévue (0..23, les vols tôt le matin sont moins retardés)
   *    - arr_hour          : tranche horaire d'arrivée prévue (0..23)
   *    - CRS_ELAPSED_TIME  : durée prévue du vol en minutes
   * 2. Les `mOrigin` premières observations météo de `Wo` (à l'aéroport de départ Ao) :
   *    O(Ao, tsd), O(Ao, tsd - 1h), ..., O(Ao, tsd - (mOrigin - 1)h)
   *    Chaque observation apporte 8 valeurs numériques : (T, H, Wd, Ws, P, S_encodé, V, D_encodé).
   * 3. Les `nDest` premières observations météo de `Wd` (à l'aéroport de destination Ad) :
   *    O(Ad, tsa), O(Ad, tsa - 1h), ..., O(Ad, tsa - (nDest - 1)h)
   *    Chaque observation apporte également 8 valeurs numériques.
   *
   * Note : si `mOrigin = 0` et `nDest = 0`, le modèle n'utilise que les 7 caractéristiques du vol
   * (dans l'article, cela donne déjà 69.1% de précision car l'algorithme apprend les schémas de retard
   * propres à certains aéroports, jours et heures !).
   */
  def extractFeatures(df: DataFrame, mOrigin: Int, nDest: Int): DataFrame = {
    val buildVectorUdf: UserDefinedFunction = udf(
      (
        originId: Int,
        destId: Int,
        dayOfMonth: Int,
        dayOfWeek: Int,
        depHour: Int,
        arrHour: Int,
        elapsed: Double,
        wo: Seq[Row],
        wd: Seq[Row]
      ) => {
        val flightFeats = Array(
          originId.toDouble,
          destId.toDouble,
          dayOfMonth.toDouble,
          dayOfWeek.toDouble,
          depHour.toDouble,
          arrHour.toDouble,
          elapsed
        )

        // Convertit les `count` premières observations d'un tableau météo (Wo ou Wd) en tableau de Doubles
        def obsToFeatures(obsSeq: Seq[Row], count: Int): Array[Double] = {
          val buffer = new Array[Double](count * 8)
          var i = 0
          while (i < count) {
            val base = i * 8
            if (obsSeq != null && i < obsSeq.size && obsSeq(i) != null) {
              val r = obsSeq(i)
              buffer(base + 0) = r.getDouble(1) // T  : Température
              buffer(base + 1) = r.getDouble(2) // H  : Humidité
              buffer(base + 2) = r.getDouble(3) // Wd : Direction du vent
              buffer(base + 3) = r.getDouble(4) // Ws : Vitesse du vent
              buffer(base + 4) = r.getDouble(5) // P  : Pression barométrique
              buffer(base + 5) = encodeSkyCondition(r.getString(6)) // S : État du ciel (encodé)
              buffer(base + 6) = r.getDouble(7) // V  : Visibilité
              buffer(base + 7) = encodeWeatherDesc(r.getString(8))  // D : Phénomène météo (encodé)
            }
            i += 1
          }
          buffer
        }

        val originWeatherFeats = obsToFeatures(wo, mOrigin)
        val destWeatherFeats   = obsToFeatures(wd, nDest)

        Vectors.dense(flightFeats ++ originWeatherFeats ++ destWeatherFeats)
      }
    )

    df.withColumn("day_of_month", dayofmonth(col("tsd")))
      .withColumn("day_of_week", dayofweek(col("tsd")))
      .withColumn("dep_hour", hour(col("tsd")))
      .withColumn("arr_hour", hour(col("tsa")))
      .withColumn(
        "features",
        buildVectorUdf(
          col("ORIGIN_AIRPORT_ID"),
          col("DEST_AIRPORT_ID"),
          col("day_of_month"),
          col("day_of_week"),
          col("dep_hour"),
          col("arr_hour"),
          col("CRS_ELAPSED_TIME"),
          col("Wo"),
          col("Wd")
        )
      )
      .select("label", "features")
  }

  /**
   * Encodage ordinal de l'état du ciel METAR (`SkyCondition`, champ S de la Définition 2.2) :
   * Au lieu d'utiliser un StringIndexer qui créerait des milliers de modalités textuelles
   * (ex. "FEW025 BKN080 OVC120"), on extrait la couche nuageuse la plus dense selon l'échelle METAR :
   * - 0.0 : CLR / SKC (Ciel dégagé, 0 octas)
   * - 1.0 : FEW       (Quelques nuages, 1 à 2 octas)
   * - 2.0 : SCT       (Nuages épars, 3 à 4 octas)
   * - 3.0 : BKN       (Ciel fragmenté, 5 à 7 octas)
   * - 4.0 : OVC / VV  (Ciel couvert 8 octas ou visibilité verticale réduite)
   */
  private def encodeSkyCondition(sky: String): Double = {
    if (sky == null) 0.0
    else {
      val s = sky.toUpperCase
      if (s.contains("OVC") || s.contains("VV")) 4.0
      else if (s.contains("BKN")) 3.0
      else if (s.contains("SCT")) 2.0
      else if (s.contains("FEW")) 1.0
      else 0.0
    }
  }

  /**
   * Encodage ordinal du descripteur de phénomènes météo METAR (`WeatherType`, champ D de la Déf. 2.2) :
   * Comme expliqué en Section 2.3 (page 5), un code METAR combine un phénomène (RA = pluie, SN = neige,
   * TS = orage, GR = grêle, FG = brouillard, etc.) et un qualificatif d'intensité (`+` fort, `-` faible).
   * On le traduit en un score de sévérité croissant pour faciliter les coupures des arbres de décision :
   * - 0.0 : Aucun phénomène ("NONE")
   * - 1.0 : Obscurcissement (FG = brouillard, BR = brume, HZ = brume sèche)
   * - 2.0 : Précipitations liquides (RA = pluie, DZ = bruine)
   * - 3.0 : Précipitations hivernales / givrantes (SN = neige, FZ = verglas, PL = grésil)
   * - 4.0 : Phénomènes convectifs violents (TS = orage, GR = grêle, SQ = grain)
   * Modulé par l'intensité : +0.5 si fort (`+`, ex. `+SN` -> 3.5) et -0.25 si faible (`-`, ex. `-RA` -> 1.75).
   */
  private def encodeWeatherDesc(desc: String): Double = {
    if (desc == null || desc == "NONE" || desc.isEmpty) 0.0
    else {
      val d = desc.toUpperCase
      val baseSeverity =
        if (d.contains("TS") || d.contains("GR") || d.contains("SQ")) 4.0
        else if (d.contains("SN") || d.contains("FZ") || d.contains("PL")) 3.0
        else if (d.contains("RA") || d.contains("DZ")) 2.0
        else if (d.contains("FG") || d.contains("BR") || d.contains("HZ")) 1.0
        else 0.5
      val intensityMod =
        if (d.contains("+")) 0.5
        else if (d.contains("-")) -0.25
        else 0.0
      baseSeverity + intensityMod
    }
  }

  /**
   * Modèle 1 : Arbre de Décision (Spark ML `DecisionTreeClassifier`)
   * Requis par les consignes du projet ("Decision-tree, use Spark ML") et comparé en Section 4.3 (C4.5).
   */
  def trainAndEvaluateDecisionTree(trainDf: DataFrame, testDf: DataFrame): EvaluationMetrics = {
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(10)
      .setMaxBins(64)
      .setSeed(42L)

    val model = dt.fit(trainDf)
    val predictions = model.transform(testDf)
    evaluatePredictions("Decision Tree (Spark ML)", predictions)
  }

  /**
   * Modèle 2 : Forêt Aléatoire (Spark ML `RandomForestClassifier`)
   * Modèle principal retenu dans l'article (Section 4.3, Figure 5) : combine les prédictions de plusieurs
   * arbres de décision construits par bagging et sélection aléatoire d'attributs (vote majoritaire).
   */
  def trainAndEvaluateRandomForest(trainDf: DataFrame, testDf: DataFrame): EvaluationMetrics = {
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(10)
      .setMaxBins(64)
      .setSeed(42L)

    val model = rf.fit(trainDf)
    val predictions = model.transform(testDf)
    evaluatePredictions("Random Forest (Spark ML)", predictions)
  }

  /**
   * Modèle 3 : Arbres Boostés par Gradient (Spark ML `GBTClassifier`)
   * Modèle additionnel ("Use of other models are welcome") : contrairement à Random Forest qui construit
   * des arbres indépendants en parallèle, GBT construit des arbres séquentiellement pour corriger
   * les erreurs résiduelles des arbres précédents.
   */
  def trainAndEvaluateGBT(trainDf: DataFrame, testDf: DataFrame): EvaluationMetrics = {
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(20)
      .setMaxDepth(6)
      .setMaxBins(64)
      .setSeed(42L)

    val model = gbt.fit(trainDf)
    val predictions = model.transform(testDf)
    evaluatePredictions("Gradient-Boosted Trees", predictions)
  }

  /**
   * Calcul de la matrice de confusion et des indicateurs de performance (Section 2.4, Tableau II, Éq. 1 & 2) :
   *
   * Attention à la convention de nommage de l'article (Tableau II, page 6) :
   * - Les auteurs appellent "Positif" un vol À L'HEURE (`label == 0.0`) et "Négatif" un vol EN RETARD (`label == 1.0`).
   *   - TP (Vrai Positif) : vol à l'heure (0.0) correctement prédit à l'heure (0.0)
   *   - FN (Faux Négatif) : vol à l'heure (0.0) prédit à tort en retard (1.0)
   *   - FP (Faux Positif) : vol en retard (1.0) prédit à tort à l'heure (0.0)
   *   - TN (Vrai Négatif) : vol en retard (1.0) correctement prédit en retard (1.0)
   * - Formules exactes de l'article :
   *   - Équation (1) : Acc   = (TP + TN) / (TP + TN + FP + FN)
   *   - Équation (2) : Rec_o = TP / (TP + FN)   (Rappel sur la classe "à l'heure")
   *   - Équation (2) : Rec_d = TN / (TN + FP)   (Rappel sur la classe "en retard")
   */
  def evaluatePredictions(modelName: String, predictions: DataFrame): EvaluationMetrics = {
    val counts = predictions
      .select(
        sum(when(col("label") === 0.0 && col("prediction") === 0.0, 1L).otherwise(0L)).as("TP"),
        sum(when(col("label") === 0.0 && col("prediction") === 1.0, 1L).otherwise(0L)).as("FN"),
        sum(when(col("label") === 1.0 && col("prediction") === 0.0, 1L).otherwise(0L)).as("FP"),
        sum(when(col("label") === 1.0 && col("prediction") === 1.0, 1L).otherwise(0L)).as("TN")
      )
      .first()

    val tp = if (counts.isNullAt(0)) 0L else counts.getLong(0)
    val fn = if (counts.isNullAt(1)) 0L else counts.getLong(1)
    val fp = if (counts.isNullAt(2)) 0L else counts.getLong(2)
    val tn = if (counts.isNullAt(3)) 0L else counts.getLong(3)

    val total = (tp + tn + fp + fn).toDouble
    val acc   = if (total > 0) (tp + tn) * 100.0 / total else 0.0
    val reco  = if (tp + fn > 0) tp * 100.0 / (tp + fn).toDouble else 0.0
    val recd  = if (tn + fp > 0) tn * 100.0 / (tn + fp).toDouble else 0.0

    val rocEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val aucRoc = if (total > 0) rocEvaluator.evaluate(predictions) else 0.0

    EvaluationMetrics(modelName, tp, tn, fp, fn, acc, reco, recd, aucRoc)
  }

  /** Affiche un tableau comparatif formaté des indicateurs (Acc, Rec_o, Rec_d, AUC-ROC et matrice de confusion) */
  def printEvaluationSummary(metricsList: Seq[EvaluationMetrics]): Unit = {
    println("\n==========================================================================================")
    println(f"${"Modèle"}%-26s | ${"Acc (%)"}%8s | ${"Rec_o (%)"}%9s | ${"Rec_d (%)"}%9s | ${"AUC-ROC"}%7s | ${"TP / TN / FP / FN"}")
    println("------------------------------------------------------------------------------------------")
    metricsList.foreach { m =>
      println(
        f"${m.modelName}%-26s | ${m.accuracy}%8.2f | ${m.recallOnTime}%9.2f | ${m.recallDelayed}%9.2f | ${m.aucRoc}%7.4f | ${m.tp}/${m.tn}/${m.fp}/${m.fn}"
      )
    }
    println("==========================================================================================\n")
  }
}
