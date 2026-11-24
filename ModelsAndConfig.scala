package tist.flightdelay

/**
 * FICHIER 1 SUR 6 : STRUCTURES DE DONNÉES ET CONFIGURATION
 *
 * Les « fiches » (case classes) qui circulent dans le pipeline :
 *   AirportTimezone     : un aéroport, sa station météo et son fuseau
 *   FlightRecord        : un vol nettoyé (table FT) + son étiquette D1..D4 (RG-DEF, RG-CIB)
 *   WeatherSlot         : un bulletin météo (table OT)
 *   JoinedFlightWeather : un vol + 13 bulletins au départ + 13 à l'arrivée (table JT)
 *   ConfusionMatrix     : TP / FN / FP / TN et métriques (Tableau II)
 *   PipelineConfig      : les paramètres de la ligne de commande
 */

// ============================================================================
// 1. AÉROPORT (fichier wban_airport_timezone.csv)
// ============================================================================

case class AirportTimezone(airportId: Int, wban: Int, timeZoneOffsetHours: Int, observesDst: Boolean)

object AirportTimezone {
  /** Aéroports d'Arizona : pas d'heure d'été. */
  val ArizonaAirportIds: Set[Int] = Set(14107, 15376, 11695, 16218)

  /** RG-IMP-02 : heure d'été partout, sauf Arizona, Hawaï (fuseau −10) et Porto Rico (fuseau −4). */
  def create(airportId: Int, wban: Int, tz: Int): AirportTimezone =
    AirportTimezone(airportId, wban, tz, observesDst = !(ArizonaAirportIds(airportId) || tz == -10 || tz == -4))
}

// ============================================================================
// 2. VOL NETTOYÉ (table FT)
// ============================================================================

/**
 * RG-MOD-01 : les 5 premières informations sont connues avant le vol et servent de variables.
 * Les trois retards (arrDelay, weatherDelay, nasDelay) ne servent qu'à l'étiquette.
 */
case class FlightRecord(
    originAirportId: Int,
    destAirportId: Int,
    dayOfWeek: Int,
    originEpochDay: Int,
    destEpochDay: Int,
    crsDepTimeMinutes: Int,
    crsArrTimeMinutes: Int,
    crsElapsedMinutes: Double,
    scheduledDepEpochSec: Long,
    scheduledArrEpochSec: Long,
    arrDelayMinutes: Double,
    weatherDelayMinutes: Double,
    nasDelayMinutes: Double,
    // Les deux offsets sont en heure STANDARD (sans DST), comme les relevés QCLCD.
    // Ils permettent de comparer une observation à destination au vrai instant du départ.
    originStandardUtcOffsetHours: Int,
    destStandardUtcOffsetHours: Int
) {

  /**
   * Étiquette du vol pour un jeu cible et un seuil Th :
   *   0.0 = à l'heure (RG-DEF-01), 1.0 = en retard et retenu (RG-DEF-02 + RG-CIB), −1.0 = en retard mais écarté.
   */
  def getDatasetLabel(dataset: String, thresholdMinutes: Int): Double = {
    if (arrDelayMinutes < thresholdMinutes) return 0.0
    val w = weatherDelayMinutes
    val n = nasDelayMinutes
    val retained = dataset.toUpperCase match {
      case "D1" => (w > 0.0 || n > 0.0) && w + n + 1e-6 >= arrDelayMinutes // RG-CIB-01 : uniquement météo et/ou NAS
      case "D2" => w > 0.0 || n >= thresholdMinutes                         // RG-CIB-02 : météo, ou NAS ≥ Th
      case "D3" => w > 0.0 || n > 0.0                                       // RG-CIB-03 : météo ou NAS, non exclusif
      case "D4" => true                                                    // RG-CIB-04 : tous les retards
      case other => throw new IllegalArgumentException(s"Jeu cible inconnu : $other")
    }
    if (retained) 1.0 else -1.0
  }
}

// ============================================================================
// 3. BULLETIN MÉTÉO (table OT)
// ============================================================================

/** RG-IMP-06 : valeur manquante = −999.0 pour un nombre, "MISSING" pour un texte. */
case class WeatherSlot(
    airportId: Int,
    obsEpochDay: Int,
    obsEpochSec: Long,
    temperatureF: Double,
    relativeHumidity: Double,
    windDirectionDeg: Double,
    windSpeedKt: Double,
    stationPressureInHg: Double,
    skyCondition: String,
    skyConditionScore: Double,
    visibilityMiles: Double,
    weatherType: String,
    missingCount: Int
)

object WeatherSlot {
  val NumericSentinel: Double     = -999.0
  val CategoricalSentinel: String = "MISSING"
  val WindowSizeHours: Int        = 13 // de t à t − 12 h

  /** Bulletin vide, utilisé quand aucun bulletin ne convient. */
  val MissingSlot: WeatherSlot = WeatherSlot(-1, 0, 0L, NumericSentinel, NumericSentinel, NumericSentinel, NumericSentinel,
    NumericSentinel, CategoricalSentinel, NumericSentinel, NumericSentinel, CategoricalSentinel, missingCount = 8)
}

// ============================================================================
// 4. VOL ENRICHI (table JT = {F, W_o, W_d, C})
// ============================================================================

/** `label` est écrit par BalancedSampler.filterTargetDataset (0.0 par défaut). */
case class JoinedFlightWeather(
    flight: FlightRecord,
    originWeather: Array[WeatherSlot],
    destWeather: Array[WeatherSlot],
    label: Double = 0.0
)

// ============================================================================
// 5. ÉVALUATION (Tableau II : le positif est « à l'heure »)
// ============================================================================

case class ConfusionMatrix(tp: Long, fn: Long, fp: Long, tn: Long) {
  private def ratio(a: Long, b: Long): Double = if (b == 0L) 0.0 else a.toDouble / b
  def total: Long          = tp + fn + fp + tn
  def accuracy: Double     = ratio(tp + tn, total) // Acc
  def onTimeRecall: Double = ratio(tp, tp + fn)    // Rec_o
  def delayRecall: Double  = ratio(tn, tn + fp)    // Rec_d
}

case class EvaluationMetrics(confusionMatrix: ConfusionMatrix, aucRoc: Double, trainCount: Long, testCount: Long, elapsedSeconds: Double) {
  def accuracy: Double     = confusionMatrix.accuracy
  def onTimeRecall: Double = confusionMatrix.onTimeRecall
  def delayRecall: Double  = confusionMatrix.delayRecall
}

case class ModelComparisonEntry(modelName: String, familyDescription: String, metrics: EvaluationMetrics)

// ============================================================================
// 6. CONFIGURATION
// ============================================================================

/** RG-MOD-02 : indices des heures météo utilisées au départ (W_o) et à l'arrivée (W_d). */
case class WeatherFeatureSpec(label: String, originOffsets: Array[Int], destOffsets: Array[Int])

object WeatherFeatureSpec {
  val NoWeather: WeatherFeatureSpec          = WeatherFeatureSpec("M_{d,0,0} (Sans météo)", Array.emptyIntArray, Array.emptyIntArray)
  val OptimumSymmetric7h: WeatherFeatureSpec = hourly(7, 7)
  val Subsampled3Hour: WeatherFeatureSpec    = WeatherFeatureSpec("Sous-échantillonné 3h (k={0,3,6})", Array(0, 3, 6), Array(0, 3, 6))

  def hourly(m: Int, n: Int): WeatherFeatureSpec =
    WeatherFeatureSpec(s"M_{d,$m,$n} (Horaire 1h)", (0 until m).toArray, (0 until n).toArray)
}

/** Paramètres du programme (valeurs par défaut = configuration optimale de l'article). */
case class PipelineConfig(
    dataDir: String = "flights_data",
    flightsGlob: String = "",
    weatherGlob: String = "",
    timezoneFile: String = "",
    targetDataset: String = "D2",
    thresholdMinutes: Int = 60,
    mOriginHours: Int = 7,
    nDestHours: Int = 7,
    subsampled3h: Boolean = false,
    repetitions: Int = 1,
    numTrees: Int = 50,
    maxDepth: Int = 12,
    maxBins: Int = 128,
    numJoinPartitions: Int = 64,
    localClockMode: Boolean = false,
    strictCausalMode: Boolean = false,
    seed: Long = 42L,
    compareModels: Boolean = true,
    runValidationSuite: Boolean = false,
    runSelfTest: Boolean = false,
    // Mesure facultative mais recommandée : DT sur chacun des mêmes tirages que le RF.
    evaluateDecisionTreeEveryRepetition: Boolean = false,
    sparkMaster: String = "local[*]"
) {
  // RG-DEF-03 : vérifié à chaque création, donc aussi après la lecture de la ligne de commande
  require(Set(15, 30, 45, 60, 90).contains(thresholdMinutes), s"RG-DEF-03 : Seuil Th=$thresholdMinutes invalide")
  require(Set("D1", "D2", "D3", "D4").contains(targetDataset.toUpperCase), s"Jeu cible $targetDataset invalide")
  require(mOriginHours >= 0 && mOriginHours <= WeatherSlot.WindowSizeHours, "--m doit appartenir à [0,13]")
  require(nDestHours >= 0 && nDestHours <= WeatherSlot.WindowSizeHours, "--n doit appartenir à [0,13]")
  require(repetitions >= 1, "--repetitions doit être >= 1")
  require(numJoinPartitions > 0, "--partitions doit être >= 1")
  require(numTrees >= 1 && maxDepth >= 1 && maxDepth <= 30, "Paramètres d'arbres invalides")
  require(maxBins >= 2, "--max-bins doit être >= 2")
  require(!(strictCausalMode && localClockMode),
    "Le mode causal exige --time-mode LST : les bulletins QCLCD sont en heure standard locale")

  def resolvedFlightsGlob: String  = if (flightsGlob.nonEmpty) flightsGlob else s"$dataDir/Flights/*.csv"
  def resolvedWeatherGlob: String  = if (weatherGlob.nonEmpty) weatherGlob else s"$dataDir/Weather/*hourly.txt"
  def resolvedTimezoneFile: String = if (timezoneFile.nonEmpty) timezoneFile else s"$dataDir/wban_airport_timezone.csv"

  def activeWeatherSpec: WeatherFeatureSpec =
    if (subsampled3h) WeatherFeatureSpec.Subsampled3Hour else WeatherFeatureSpec.hourly(mOriginHours, nDestHours)
}

object PipelineConfig {
  /** Lit la ligne de commande, ex. `--dataset D2 --threshold 60 --repetitions 10`. */
  def parseArgs(args: Array[String]): PipelineConfig = {
    @annotation.tailrec
    def loop(rest: List[String], c: PipelineConfig): PipelineConfig = rest match {
      case Nil                                    => c
      case "--data-dir" :: v :: t                 => loop(t, c.copy(dataDir = v))
      case "--flights-glob" :: v :: t             => loop(t, c.copy(flightsGlob = v))
      case "--weather-glob" :: v :: t             => loop(t, c.copy(weatherGlob = v))
      case "--timezone-file" :: v :: t            => loop(t, c.copy(timezoneFile = v))
      case "--dataset" :: v :: t                  => loop(t, c.copy(targetDataset = v.toUpperCase))
      case ("--threshold" | "--th") :: v :: t     => loop(t, c.copy(thresholdMinutes = v.toInt))
      case "--m" :: v :: t                        => loop(t, c.copy(mOriginHours = v.toInt))
      case "--n" :: v :: t                        => loop(t, c.copy(nDestHours = v.toInt))
      case "--subsampled-3h" :: t                 => loop(t, c.copy(subsampled3h = true))
      case "--repetitions" :: v :: t              => loop(t, c.copy(repetitions = v.toInt))
      case "--num-trees" :: v :: t                => loop(t, c.copy(numTrees = v.toInt))
      case "--max-depth" :: v :: t                => loop(t, c.copy(maxDepth = v.toInt))
      case "--max-bins" :: v :: t                 => loop(t, c.copy(maxBins = v.toInt))
      case "--partitions" :: v :: t               => loop(t, c.copy(numJoinPartitions = v.toInt))
      case "--time-mode" :: v :: t                =>
        val isLocal = v.toUpperCase match {
          case "LOCAL" | "LOCAL_CLOCK" => true
          case "LST" | "STANDARD" | "EXACT_LST_UTC" => false
          case _ => throw new IllegalArgumentException(s"--time-mode inconnu : $v (LOCAL ou LST)")
        }
        loop(t, c.copy(localClockMode = isLocal))
      case "--causal-mode" :: v :: t              =>
        val isCausal = v.toUpperCase match {
          case "CAUSAL" | "C01" | "STRICT_CAUSAL" => true
          case "STANDARD" | "OFF" | "FALSE" => false
          case _ => throw new IllegalArgumentException(s"--causal-mode inconnu : $v")
        }
        loop(t, c.copy(strictCausalMode = isCausal))
      case "--seed" :: v :: t                     => loop(t, c.copy(seed = v.toLong))
      case "--compare-models" :: t                => loop(t, c.copy(compareModels = true))
      case "--no-model-comparison" :: t           => loop(t, c.copy(compareModels = false))
      case "--validation-suite" :: t              => loop(t, c.copy(runValidationSuite = true))
      case "--self-test" :: t                     => loop(t, c.copy(runSelfTest = true))
      case "--dt-every-repetition" :: t           => loop(t, c.copy(evaluateDecisionTreeEveryRepetition = true))
      case "--master" :: v :: t                   => loop(t, c.copy(sparkMaster = v))
      case other :: _                             => throw new IllegalArgumentException(s"Argument inconnu ou valeur manquante : $other")
    }
    loop(args.toList, PipelineConfig())
  }
}
