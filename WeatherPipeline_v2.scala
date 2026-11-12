package flight

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

// =====================================================================
// MÉTÉO — préparation de la table OT (Belcastro et al. 2016, déf. 2.2 et §4.1) :
// 8 attributs de O, conversion stricte des valeurs, observations à l'heure exacte.
// =====================================================================

object WeatherPipeline_v2 {

  // Attributs de O = <A, t, T, H, Wd, Ws, P, S, V, D> (déf. 2.2 de l'article)
  //      HourlyPrecip est retiré : la précipitation est portée par D (codes METAR).
  val NumericCols: Seq[String] = Seq(
    "DryBulbFarenheit",  // T
    "RelativeHumidity",  // H
    "WindDirection",     // Wd
    "WindSpeed",         // Ws
    "StationPressure",   // P
    "Visibility"         // V
  )
  val CategoricalCols: Seq[String] = Seq(
    "SkyCondition",      // S
    "WeatherType"        // D
  )

  // Domaines physiquement possibles : on écarte l'IMPOSSIBLE, pas le RARE.
  //      Bornes volontairement larges (min/max vérifiés par le contrôle 4).
  val Bounds: Map[String, (Double, Double)] = Map(
    "DryBulbFarenheit" -> (-80.0, 135.0),  // records US : -80 °F (Alaska), 134 °F (Death Valley)
    "RelativeHumidity" -> (0.0, 100.0),    // %
    "WindDirection"    -> (0.0, 360.0),    // degrés (0 = calme, 360 = nord)
    "WindSpeed"        -> (0.0, 150.0),    // large : couvre mph ou nœuds (unité non précisée)
    "StationPressure"  -> (18.0, 32.0),    // inHg ; ~21 inHg aux aéroports les plus hauts
    "Visibility"       -> (0.0, 100.0)     // miles (max observé en avril 2013 : 80)
  )

  // Suffixes qualité reconnus (voir le profilage printProfile).
  //      Un suffixe absent de cette liste => valeur "unknown" (null, comptée).
  val KnownSuffixes: Seq[String] = Seq("s", "V")

  // Codes "vent de direction variable" : VRB (doc QCLCD) et VR (constaté dans 201304,
  //      6 345 valeurs). Direction non définie -> null ; la vitesse reste valide.
  val VariableWindCodes: Seq[String] = Seq("VRB", "VR")

  // WeatherType vide = "rien à signaler" dans un bulletin METAR : c'est une
  //      VALEUR (aucun phénomène), pas un manque. Seul "M" est un vrai manquant.
  val NoPhenomenon: String = "NONE"

  // Motifs (regex Java), appliqués sur la valeur après trim.
  // Nombre : -4 | 29.92 | 0.25 | .5 | +3
  private val NumRe      = "[-+]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)"
  val PureRe: String     = "^" + NumRe + "$"                       // nombre seul
  val SuffixedRe: String = "^(" + NumRe + ")([^0-9.\\s]+)$"         // nombre + suffixe collé

  // ------------------------------------------------------------------
  // Petites briques réutilisées par le profilage et le nettoyage
  // ------------------------------------------------------------------

  /** Famille d'une valeur brute (déjà trimée) — pour le profilage. */
  private def family(raw: Column): Column =
    when(raw.isNull || raw === "", "empty")
      .when(raw === "M", "M")
      .when(raw.isin(VariableWindCodes: _*), "VRB")
      .when(raw.rlike(PureRe), "pure")
      .when(raw.rlike(SuffixedRe), "suffix")
      .otherwise("other")

  /** Partie numérique : nombre pur, ou nombre situé devant le suffixe ; null sinon. */
  private def numericPart(raw: Column): Column =
    when(raw.rlike(PureRe), raw.cast("double"))
      .when(raw.rlike(SuffixedRe), regexp_extract(raw, SuffixedRe, 1).cast("double"))

  /** Suffixe collé au nombre ("s", "V", ...) ; null si la valeur n'a pas de suffixe. */
  private def suffixOf(raw: Column): Column =
    when(raw.rlike(SuffixedRe), regexp_extract(raw, SuffixedRe, 2))

  /**
   * Raison attribuée à chaque valeur d'une colonne numérique.
   * Seules "pure" et "suffix" donnent une valeur ; toutes les autres donnent null.
   * L'ordre des tests compte : codes de manque AVANT l'extraction.
   */
  private def reason(c: String, dropSuffixes: Seq[String]): Column = {
    val raw = trim(col(c))
    val (lo, hi) = Bounds(c)
    val num = numericPart(raw)
    val sfx = suffixOf(raw)

    def inRange(tag: String): Column =
      when(num.between(lo, hi), lit(tag)).otherwise(lit("out_of_range"))

    val isDropped = if (dropSuffixes.isEmpty) lit(false) else sfx.isin(dropSuffixes: _*)
    val isKnown   = sfx.isin(KnownSuffixes: _*)

    when(raw.isNull || raw === "", lit("empty"))                          // manquant
      .when(raw === "M", lit("M"))                                         // manquant (code officiel)
      .when(raw.isin(VariableWindCodes: _*), lit("VRB"))                                     // vent variable
      .when(raw.rlike(PureRe), inRange("pure"))                            // nombre normal
      .when(raw.rlike(SuffixedRe) && isDropped, lit("suffix_dropped"))     // suffixe écarté
      .when(raw.rlike(SuffixedRe) && isKnown, inRange("suffix"))           // suffixe connu : valeur récupérée
      .otherwise(lit("unknown"))                                           // forme inattendue
  }

  // ------------------------------------------------------------------
  // Étapes du pipeline météo
  // ------------------------------------------------------------------

  /** Étape 1 : garder les stations d'aéroport. WBAN casté en int des deux côtés
   *  (la météo est lue en texte, la table de correspondance en int). */
  def joinWithMapping(weather: DataFrame, mapping: DataFrame): DataFrame =
    weather.withColumn("WBAN", col("WBAN").cast("int"))
      .join(mapping.withColumn("WBAN", col("WBAN").cast("int")), Seq("WBAN"), "inner")

  /** Étape 2 : A, Date, Time + les 8 attributs de O (sans HourlyPrecip). */
  def selectKeyColumns(df: DataFrame): DataFrame =
    df.select((Seq("AirportID", "Date", "Time") ++ NumericCols ++ CategoricalCols).map(col): _*)

  /**
   * Étape 3, version DIAGNOSTIC : pour chaque colonne numérique c, produit
   *   c__raw    : valeur brute (trimée)
   *   c         : valeur nettoyée (double ou null)
   *   c__reason : pourquoi (pure, suffix, suffix_dropped, M, empty, VRB, unknown, out_of_range)
   * D (WeatherType) : vide -> "NONE" (aucun phénomène), M -> null.
   * S (SkyCondition)  : M / vide -> null (le ciel dégagé est noté explicitement "CLR").
   */
  def cleanAndCastWithDiagnostics(df: DataFrame, dropSuffixes: Seq[String]): DataFrame = {
    val numericCols: Seq[Column] = NumericCols.flatMap { c =>
      val r = reason(c, dropSuffixes)
      Seq(
        trim(col(c)).as(s"${c}__raw"),
        when(r.isin("pure", "suffix"), numericPart(trim(col(c)))).as(c),
        r.as(s"${c}__reason")
      )
    }
    val sky = trim(col("SkyCondition"))
    val wx  = trim(col("WeatherType"))
    val categoricalCols: Seq[Column] = Seq(
      // S : M / vide -> null (vrais manquants : le ciel dégagé est noté CLR)
      when(sky.isNull || sky === "" || sky === "M", lit(null).cast("string"))
        .otherwise(sky).as("SkyCondition"),
      // D : vide (lu comme null par Spark CSV) -> NONE ; M -> null
      when(wx.isNull || wx === "", lit(NoPhenomenon))
        .when(wx === "M", lit(null).cast("string"))
        .otherwise(wx).as("WeatherType")
    )
    df.select((Seq(col("AirportID"), col("Date"), col("Time")) ++ numericCols ++ categoricalCols): _*)
  }

  /** Étape 3, version PIPELINE : identique, sans les colonnes de diagnostic. */
  def cleanAndCast(df: DataFrame, dropSuffixes: Seq[String]): DataFrame = {
    val diag = cleanAndCastWithDiagnostics(df, dropSuffixes)
    diag.drop(diag.columns.filter(_.contains("__")): _*)
  }

  /** Pipeline météo complet (table OT). */
  def preprocess(weatherRaw: DataFrame, mapping: DataFrame, dropSuffixes: Seq[String]): DataFrame = {
    val step1 = joinWithMapping(weatherRaw, mapping)
    val step2 = selectKeyColumns(step1)                     // 8 attributs de O
    val step3 = cleanAndCast(step2, dropSuffixes)           // conversion stricte, D vide -> NONE
    val step4 = WeatherPipeline.buildTimestamp(step3)       // horodatage
    // Toutes les observations, heure exacte (08:51 reste 08:51).
    //      On ne retire que les lignes strictement identiques (copies).
    val step5 = step4.dropDuplicates()
    // Même station + même minute mais valeurs différentes (97 cas en avril 2013) :
    //      on garde UN bulletin réel, le plus complet ; ordre fixe en cas d'égalité
    //      -> sélection reproductible, aucune valeur fusionnée ni inventée.
    val step6 = resolveSameMinute(step5)
    step6
  }

  /** Une seule observation par (AirportID, obs_time) : la plus complète,
   *  puis un ordre fixe sur toutes les valeurs (déterministe). */
  def resolveSameMinute(df: DataFrame): DataFrame = {
    val valueCols = NumericCols ++ CategoricalCols
    val nMissing  = valueCols.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
    val w = org.apache.spark.sql.expressions.Window
      .partitionBy("AirportID", "obs_time")
      .orderBy((nMissing.asc +: valueCols.map(c => col(c).asc_nulls_last)): _*)
    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }

  // ------------------------------------------------------------------
  // Contrôles de la table OT à heure exacte
  // ------------------------------------------------------------------

  /**
   * before  = observations avant dédoublonnage (sortie de buildTimestamp)
   * ot      = table OT (sortie de preprocess)
   * mapping = table de correspondance AirportID / WBAN
   */
  def printControlsB6(before: DataFrame, ot: DataFrame, mapping: DataFrame): Unit = {
    println("\n--- [B6] Contrôle 1 : nombre d'observations ---")
    val strict  = before.dropDuplicates()
    val nBefore = before.count()
    val nStrict = strict.count()
    val nOT     = ot.count()
    println(f"Avant = $nBefore%d   après copies retirées = $nStrict%d   OT = $nOT%d")
    println(f"  copies identiques retirées          : ${nBefore - nStrict}%d")
    println(f"  bulletins écartés (même minute)     : ${nStrict - nOT}%d")
    println(s"Heures illisibles (obs_time null) : ${ot.filter(col("obs_time").isNull).count()}")

    println("\n--- [B6] Contrôle 2 : même aéroport + même minute (avant / après résolution) ---")
    val keysBefore = strict.groupBy("AirportID", "obs_time").count().filter(col("count") > 1).count()
    val keysAfter  = ot.groupBy("AirportID", "obs_time").count().filter(col("count") > 1).count()
    println(s"Clés en double avant résolution : $keysBefore")
    println(s"Clés en double dans OT          : $keysAfter   ${if (keysAfter == 0) "OK" else "ÉCART"}")

    println("--- [B6] Contrôle 3 : aéroports reliés à plusieurs stations WBAN (attendu : 0) ---")
    val multi = mapping.groupBy("AirportID").agg(countDistinct("WBAN").as("n_wban")).filter(col("n_wban") > 1)
    println(s"Aéroports avec plusieurs WBAN : ${multi.count()}")
    multi.show(10, truncate = false)

    println("--- [B6] Contrôle 4 : minutes des observations (attendu : pic vers :51-:56) ---")
    ot.groupBy(minute(col("obs_time")).as("minute")).count()
      .withColumn("pct", round(col("count") * 100.0 / nOT, 2))
      .orderBy(desc("count"))
      .show(10, truncate = false)
    val nOnHour = ot.filter(minute(col("obs_time")) === 0).count()
    println(f"Observations à HH:00 pile : $nOnHour%d (${nOnHour * 100.0 / nOT}%.2f%%)")

    println("\n--- [B6] Contrôle 5 : observations par aéroport et par jour ---")
    ot.groupBy(col("AirportID"), to_date(col("obs_time")).as("jour")).count()
      .agg(
        min("count").as("min"),
        percentile_approx(col("count"), lit(0.5), lit(10000)).as("mediane"),
        round(avg("count"), 1).as("moyenne"),
        max("count").as("max"))
      .show(truncate = false)
  }

  // ------------------------------------------------------------------
  // PROFILAGE des valeurs brutes (avant conversion)
  // ------------------------------------------------------------------

  /** df = sortie de selectKeyColumns (valeurs brutes en texte). */
  def printProfile(df: DataFrame): Unit = {
    val longDf = NumericCols
      .map(c => df.select(lit(c).as("column"), trim(col(c)).as("raw")))
      .reduce(_ unionByName _)
      .withColumn("family", family(col("raw")))
      .withColumn("suffix", suffixOf(col("raw")))
      .withColumn("value", numericPart(col("raw")))
      .cache()

    println("\n--- [B3] Profilage 1 : nombre de valeurs par famille ---")
    longDf.groupBy("column")
      .pivot("family", Seq("pure", "suffix", "M", "empty", "VRB", "other"))
      .count().na.fill(0)
      .orderBy("column")
      .show(truncate = false)

    println("--- [B3] Profilage 2 : suffixes rencontrés ---")
    longDf.filter(col("family") === "suffix")
      .groupBy("column", "suffix").count()
      .orderBy("column", "suffix")
      .show(50, truncate = false)

    println("--- [B3] Profilage 3 : formes inattendues (famille 'other'), les 30 plus fréquentes ---")
    longDf.filter(col("family") === "other")
      .groupBy("column", "raw").count()
      .orderBy(desc("count"))
      .show(30, truncate = false)

    // Test : les valeurs à suffixe ressemblent-elles aux valeurs normales ?
    println("--- [B3] Profilage 4 : distribution valeurs pures vs valeurs à suffixe ---")
    val pct = percentile_approx(col("value"), array(lit(0.01), lit(0.5), lit(0.99)), lit(10000))
    longDf.filter(col("family").isin("pure", "suffix"))
      .withColumn("group",
        when(col("family") === "pure", lit("pure")).otherwise(concat(lit("suffix_"), col("suffix"))))
      .groupBy("column", "group")
      .agg(
        count(lit(1)).as("n"),
        min("value").as("min"),
        pct.as("p"),
        max("value").as("max"),
        round(avg("value"), 2).as("mean"))
      .select(
        col("column"), col("group"), col("n"), col("min"),
        col("p").getItem(0).as("p01"),
        col("p").getItem(1).as("median"),
        col("p").getItem(2).as("p99"),
        col("max"), col("mean"))
      .orderBy("column", "group")
      .show(50, truncate = false)

    longDf.unpersist()
  }

  // ------------------------------------------------------------------
  // Contrôles après conversion
  // ------------------------------------------------------------------

  /**
   * diag     = sortie de cleanAndCastWithDiagnostics
   * oldClean = sortie de WeatherPipeline.cleanAndCast (conversion simple), pour comparer
   */
  def printControlsB3(diag: DataFrame, oldClean: DataFrame): Unit = {
    val okReasons = Seq("pure", "suffix")
    val reasons   = Seq("pure", "suffix", "suffix_dropped", "M", "empty", "VRB", "unknown", "out_of_range")

    val longDf = NumericCols
      .map(c => diag.select(
        lit(c).as("column"),
        col(s"${c}__raw").as("raw"),
        col(c).as("value"),
        col(s"${c}__reason").as("reason")))
      .reduce(_ unionByName _)
      .cache()

    println("\n--- [B3] Contrôle 1 : répartition des raisons par colonne ---")
    longDf.groupBy("column").pivot("reason", reasons).count().na.fill(0)
      .orderBy("column").show(truncate = false)

    println("--- [B3] Contrôle 2 : bilan des null (attendu : OK partout = aucun null silencieux) ---")
    longDf.groupBy("column").agg(
        count(lit(1)).as("total"),
        sum(when(col("value").isNull, 1).otherwise(0)).as("null_obtenus"),
        sum(when(!col("reason").isin(okReasons: _*), 1).otherwise(0)).as("null_expliques"))
      .withColumn("verdict",
        when(col("null_obtenus") === col("null_expliques"), "OK").otherwise("ÉCART"))
      .orderBy("column").show(truncate = false)

    println("--- [B3] Contrôle 3 : null avant (v1) / après (v2) ---")
    NumericCols.foreach { c =>
      val oldN = oldClean.filter(col(c).isNull).count()
      val newN = diag.filter(col(c).isNull).count()
      println(f"$c%-20s v1 = $oldN%9d   v2 = $newN%9d   récupérés = ${oldN - newN}%9d")
    }

    println("\n--- [B3] Contrôle 4 : min / max après nettoyage (tout doit être dans Bounds) ---")
    longDf.filter(col("value").isNotNull)
      .groupBy("column").agg(min("value").as("min"), max("value").as("max"))
      .orderBy("column").show(truncate = false)

    println("--- [B3] Contrôle 5 : exemples brut -> nettoyé (hors nombres purs) ---")
    longDf.filter(col("reason").isin("suffix", "suffix_dropped", "out_of_range", "unknown"))
      .groupBy("column", "reason", "raw", "value").count()
      .orderBy(col("column"), desc("count"))
      .show(40, truncate = false)

    println("--- [B3] Contrôle 6 : nombre de lignes inchangé ---")
    println(s"Lignes v1 = ${oldClean.count()}   Lignes v2 = ${diag.count()}")

    longDf.unpersist()
  }

  // ------------------------------------------------------------------
  // Profilage de S et D (valeurs brutes) + test "D vide = rien"
  // ------------------------------------------------------------------

  /** df = sortie de selectKeyColumns (valeurs brutes en texte). */
  def printProfileCategorical(df: DataFrame): Unit = {
    val total = df.count()
    CategoricalCols.foreach { c =>
      val v = trim(col(c))
      val labeled = df.select(when(v.isNull || v === "", lit("<vide>")).otherwise(v).as("valeur")).cache()
      println(s"\n--- [B4] Profilage $c : ${labeled.distinct().count()} valeurs distinctes (30 plus fréquentes) ---")
      labeled.groupBy("valeur").count()
        .withColumn("pct", round(col("count") * 100.0 / total, 2))
        .orderBy(desc("count"))
        .show(30, truncate = false)
      labeled.unpersist()
    }

    // Si "D vide" veut bien dire "aucun phénomène", la visibilité médiane
    // doit y être ~10 miles, et plus basse pour BR (brume), FG (brouillard)...
    println("--- [B4] Test : visibilité selon WeatherType (attendu : <vide> -> médiane ~10) ---")
    val d = trim(col("WeatherType"))
    df.select(
        when(d.isNull || d === "", lit("<vide>")).otherwise(d).as("WeatherType"),
        numericPart(trim(col("Visibility"))).as("V"))
      .groupBy("WeatherType")
      .agg(
        count(lit(1)).as("n"),
        percentile_approx(col("V"), lit(0.5), lit(10000)).as("V_mediane"),
        round(avg("V"), 2).as("V_moyenne"))
      .orderBy(desc("n"))
      .show(15, truncate = false)
  }

  /** raw = sortie de selectKeyColumns ; diag = sortie de cleanAndCastWithDiagnostics */
  def printControlsB4(raw: DataFrame, diag: DataFrame): Unit = {
    val r = trim(col("WeatherType"))
    val nRawEmpty = raw.filter(r.isNull || r === "").count()
    val nRawM     = raw.filter(r === "M").count()
    val nNone     = diag.filter(col("WeatherType") === NoPhenomenon).count()
    val nNull     = diag.filter(col("WeatherType").isNull).count()
    println("\n--- [B4] Contrôle WeatherType (vide -> NONE, M -> null) ---")
    println(f"vides bruts = $nRawEmpty%9d   NONE après = $nNone%9d   ${if (nRawEmpty == nNone) "OK" else "ÉCART"}")
    println(f"M bruts     = $nRawM%9d   null après = $nNull%9d   ${if (nRawM == nNull) "OK" else "ÉCART"}")
    println(s"Lignes : brut = ${raw.count()}   après = ${diag.count()}")
  }
}
