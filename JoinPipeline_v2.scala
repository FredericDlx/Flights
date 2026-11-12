package flight

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

// =====================================================================
// JOINTURE — FT × OT -> JT (Belcastro et al. 2016, §4.1)
// 13 observations au départ et à l'arrivée, bulletin le plus proche.
// =====================================================================

object JoinPipeline_v2 {

  // W_o et W_d : 13 observations, k = 0 … 12, pas de 1 heure réelle (§4.1)
  val NObs: Int = 13
  // Attributs de O retenus dans JT (déf. 2.2, hors A et t) : 6 numériques + S + D
  val Features: Seq[String] = WeatherPipeline_v2.NumericCols ++ WeatherPipeline_v2.CategoricalCols
  val Sides: Seq[String] = Seq("orig", "dest")

  private def ok(b: Boolean): String = if (b) "OK" else "ÉCART"

  /** Affiche un instant (secondes) en heure d'horloge (session Spark en UTC = pas de conversion). */
  def fmt(s: Long): String =
    java.time.LocalDateTime.ofEpochSecond(s, 0, java.time.ZoneOffset.UTC).toString

  /** Les 208 noms de colonnes météo de JT : {attribut}_{côté}_{k}. */
  def weatherColNames(nObs: Int = NObs): Seq[String] =
    for { s <- Sides; f <- Features; k <- 0 until nObs } yield s"${f}_${s}_$k"

  /** Période couverte par la météo chargée :
   *  [1er jour du mois de la 1re observation, 1er jour du mois suivant la dernière[.
   *  Avril 2013 -> [2013-04-01 00:00, 2013-05-01 00:00[ */
  def weatherPeriod(ot: DataFrame): (Long, Long) = {
    val r = ot.agg(
      trunc(min(col("obs_time")), "month").cast("timestamp").cast("long").as("start"),
      add_months(trunc(max(col("obs_time")), "month"), 1).cast("timestamp").cast("long").as("end")
    ).first()
    (r.getLong(0), r.getLong(1))
  }

  /**
   * Heures demandées : une ligne par (vol, côté, k).
   *   origine     : aéroport ORIGIN_AIRPORT_ID, heure t_sd − k h
   *   destination : aéroport DEST_AIRPORT_ID,   heure t_sa − k h
   * Les heures sont des secondes (horloge locale de l'aéroport, session UTC).
   */
  def requestedTimes(flights: DataFrame, nObs: Int = NObs): DataFrame = {
    val ks = array((0 until nObs).map(k => lit(k)): _*)
    val orig = flights.select(col("flight_id"), lit("orig").as("side"),
      col("ORIGIN_AIRPORT_ID").as("AirportID"), col("t_sd").cast("long").as("t_ref"))
    val dest = flights.select(col("flight_id"), lit("dest").as("side"),
      col("DEST_AIRPORT_ID").as("AirportID"), col("t_sa").cast("long").as("t_ref"))
    orig.unionByName(dest)
      .withColumn("k", explode(ks))
      .withColumn("req_ts", col("t_ref") - col("k").cast("long") * 3600L)
      .drop("t_ref")
  }

  /**
   * Bulletin le plus proche de chaque heure demandée.
   *
   * Méthode : pour chaque aéroport, bulletins (kind = 0) et heures demandées
   * (kind = 1) sont placés sur UNE ligne du temps. Pour chaque heure demandée :
   *   prev_ts = dernier bulletin à cette heure ou avant
   *   next_ts = premier bulletin à cette heure ou après
   * puis on garde le plus proche, l'antérieur en cas d'égalité,
   * sans écart maximal. Veille / lendemain : rien à faire (temps continu).
   * Heure demandée hors période chargée -> status out_of_period, pas de bulletin.
   * Au bord du fichier, voisin connu pas sûrement le plus proche -> edge_unknown.
   * Chaque heure est traitée seule : un bulletin peut servir plusieurs fois.
   *
   * Sortie : flight_id, side, k, AirportID, req_ts, chosen_ts,
   *          gap_min (< 0 : bulletin avant ; > 0 : après), status, tie
   */
  def matchNearest(requests: DataFrame, ot: DataFrame, periodStart: Long, periodEnd: Long): DataFrame = {
    val obs = ot.select(col("AirportID"), col("obs_time").cast("long").as("ts"), lit(0).as("kind"))
    val req = requests.select(col("AirportID"), col("req_ts").as("ts"), lit(1).as("kind"),
      col("flight_id"), col("side"), col("k"))
    val timeline = obs.unionByName(req, allowMissingColumns = true)

    val obsTs = when(col("kind") === 0, col("ts"))
    // Bulletin juste avant : ordre croissant ; à la même minute, le bulletin passe avant la demande
    val wPrev = Window.partitionBy("AirportID").orderBy(col("ts").asc, col("kind").asc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    // Bulletin juste après : même calcul sur l'ordre inverse
    // (évite un cadre "jusqu'à la fin", très lent en Spark)
    val wNext = Window.partitionBy("AirportID").orderBy(col("ts").desc, col("kind").asc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val neighbours = timeline
      .withColumn("prev_ts", last(obsTs, ignoreNulls = true).over(wPrev))
      .withColumn("next_ts", last(obsTs, ignoreNulls = true).over(wNext))
      .filter(col("kind") === 1)

    val gapPrev  = col("ts") - col("prev_ts")   // >= 0
    val gapNext  = col("next_ts") - col("ts")   // >= 0
    val inPeriod = col("ts") >= lit(periodStart) && col("ts") < lit(periodEnd)

    neighbours
      .withColumn("status",
        when(!inPeriod, lit("out_of_period"))                                  // hors période chargée
          .when(col("prev_ts").isNull && col("next_ts").isNull, lit("no_obs"))  // aéroport sans bulletin
          // Bord du fichier : le bulletin d'avant (ou d'après) n'est peut-être pas chargé : on ne garde
          // le seul voisin connu que s'il est SÛREMENT le plus proche, c.-à-d. plus près que
          // le bord du fichier. Sinon on ne peut pas savoir -> pas de bulletin.
          .when(col("prev_ts").isNull && gapNext > (col("ts") - lit(periodStart)), lit("edge_unknown"))
          .when(col("next_ts").isNull && gapPrev > (lit(periodEnd) - col("ts")), lit("edge_unknown"))
          .otherwise(lit("matched")))
      .withColumn("chosen_ts",
        when(col("status") =!= "matched", lit(null).cast("long"))
          .when(col("next_ts").isNull, col("prev_ts"))
          .when(col("prev_ts").isNull, col("next_ts"))
          .when(gapPrev <= gapNext, col("prev_ts"))                             // égalité -> antérieur
          .otherwise(col("next_ts")))
      .withColumn("tie",                                                        // vraie égalité (écart > 0)
        col("status") === "matched" && col("prev_ts").isNotNull && col("next_ts").isNotNull &&
          gapPrev === gapNext && gapPrev > 0)
      .withColumn("gap_min", (col("chosen_ts") - col("ts")) / 60.0)
      .select(col("flight_id"), col("side"), col("k"), col("AirportID"),
        col("ts").as("req_ts"), col("chosen_ts"), col("gap_min"), col("status"), col("tie"))
  }

  /** Ajoute les 8 attributs du bulletin retenu.
   *  OT a une seule ligne par (aéroport, minute) — contrôlé sur OT — donc pas de multiplication.
   *  ot_found sert au contrôle : tout bulletin retenu doit être retrouvé dans OT. */
  def attachObservations(matches: DataFrame, ot: DataFrame): DataFrame = {
    val otKeyed = ot.select(
      (Seq(col("AirportID").as("ot_airport"),
           col("obs_time").cast("long").as("ot_ts"),
           lit(true).as("ot_found")) ++ Features.map(col)): _*)
    matches
      .join(otKeyed,
        matches("AirportID") === otKeyed("ot_airport") && matches("chosen_ts") === otKeyed("ot_ts"),
        "left")
      .drop("ot_airport", "ot_ts")
  }

  /** Passage en largeur : une ligne par vol, 208 colonnes {attribut}_{côté}_{k}. */
  def toWide(matchesWithObs: DataFrame, nObs: Int = NObs): DataFrame = {
    val aggs = for { s <- Sides; f <- Features; k <- 0 until nObs } yield
      first(when(col("side") === s && col("k") === k, col(f)), ignoreNulls = true).as(s"${f}_${s}_$k")
    matchesWithObs.groupBy("flight_id").agg(aggs.head, aggs.tail: _*)
  }

  /** JT = FT + W_o + W_d. Left join : tous les vols sont gardés. */
  def joinFlightsWeather(flightsWithId: DataFrame, matchesWithObs: DataFrame, nObs: Int = NObs): DataFrame =
    flightsWithId.join(toWide(matchesWithObs, nObs), Seq("flight_id"), "left")

  // ------------------------------------------------------------------
  // Contrôles de la jointure
  // ------------------------------------------------------------------

  def printControlsJoin(ft: DataFrame, matches: DataFrame, jt: DataFrame): Unit = {
    val nFT = ft.count()

    println("\n--- [JOIN] Contrôle 1 : nombre de vols (left join : aucun vol perdu) ---")
    val nJT  = jt.count()
    val nIds = jt.select("flight_id").distinct().count()
    println(f"FT = $nFT%d   JT = $nJT%d   identifiants distincts = $nIds%d   ${ok(nFT == nJT && nIds == nJT)}")

    println("\n--- [JOIN] Contrôle 2 : heures demandées (attendu : vols × 2 côtés × 13) ---")
    val nReq     = matches.count()
    val expected = nFT * Sides.size * NObs
    println(f"heures demandées = $nReq%d   attendu = $expected%d   ${ok(nReq == expected)}")

    println("\n--- [JOIN] Contrôle 3 : statut des heures demandées ---")
    matches.groupBy("side")
      .pivot("status", Seq("matched", "out_of_period", "edge_unknown", "no_obs")).count().na.fill(0)
      .orderBy("side").show(truncate = false)
    val nLost = matches.filter(col("status") === "matched" && col("ot_found").isNull).count()
    println(s"Bulletins retenus introuvables dans OT (attendu 0) : $nLost   ${ok(nLost == 0)}")
    val nFlightsOut = matches.filter(col("status") === "out_of_period").select("flight_id").distinct().count()
    println(s"[D5] Vols ayant au moins une heure hors période : $nFlightsOut")
    val nEdge = matches.filter(col("status") === "edge_unknown").count()
    val nFlightsEdge = matches.filter(col("status") === "edge_unknown").select("flight_id").distinct().count()
    println(s"[D5-bis] Heures au bord du fichier sans plus proche certain : $nEdge (vols concernés : $nFlightsEdge)")
    println("[D5-bis] Détail par côté et par jour demandé :")
    matches.filter(col("status") === "edge_unknown")
      .groupBy(col("side"), to_date(from_unixtime(col("req_ts"))).as("jour_demande")).count()
      .orderBy("side", "jour_demande").show(truncate = false)
    println("[D5] Heures hors période, par côté et par jour demandé :")
    matches.filter(col("status") === "out_of_period")
      .groupBy(col("side"), to_date(from_unixtime(col("req_ts"))).as("jour_demande")).count()
      .orderBy("side", "jour_demande").show(truncate = false)

    println("--- [JOIN] Contrôle 4 : écart bulletin retenu − heure demandée (minutes) [D2, D3, D4] ---")
    val m      = matches.filter(col("status") === "matched")
    val absGap = abs(col("gap_min"))
    m.groupBy("side").agg(
        count(lit(1)).as("n"),
        percentile_approx(absGap, lit(0.5), lit(10000)).as("mediane"),
        percentile_approx(absGap, lit(0.95), lit(10000)).as("p95"),
        max(absGap).as("max"),
        sum(when(absGap > 30, 1).otherwise(0)).as("gt_30min"),
        sum(when(absGap > 60, 1).otherwise(0)).as("gt_1h"),
        sum(when(absGap > 180, 1).otherwise(0)).as("gt_3h"),
        sum(when(col("gap_min") > 0, 1).otherwise(0)).as("posterieur"),
        sum(when(col("gap_min") === 0, 1).otherwise(0)).as("meme_minute"),
        sum(when(col("tie"), 1).otherwise(0)).as("egalites_D3"))
      .orderBy("side").show(truncate = false)
    println("Les 5 plus grands écarts :")
    m.orderBy(absGap.desc)
      .select(col("side"), col("AirportID"),
        from_unixtime(col("req_ts"), "yyyy-MM-dd HH:mm").as("heure_demandee"),
        from_unixtime(col("chosen_ts"), "yyyy-MM-dd HH:mm").as("bulletin_retenu"),
        col("gap_min"))
      .show(5, truncate = false)

    println("--- [JOIN] Contrôle 5 : même bulletin pour plusieurs heures d'un vol [D6] ---")
    val reuse = m.groupBy("flight_id", "side", "AirportID")
      .agg(count("chosen_ts").as("n"), countDistinct("chosen_ts").as("nd"))
      .filter(col("nd") < col("n"))
    println(s"Vols concernés (au moins un côté) : ${reuse.select("flight_id").distinct().count()}")
    println("Aéroports les plus concernés :")
    reuse.groupBy("side", "AirportID").count().orderBy(desc("count")).show(10, truncate = false)

    println("--- [JOIN] Contrôle 6 : bilan des null par attribut (format long -> format large) ---")
    val longAggs = Features.flatMap { f =>
      Seq(
        sum(when(col("status") =!= "matched", 1).otherwise(0)).as(s"${f}__unmatched"),
        sum(when(col("status") === "matched" && col(f).isNull, 1).otherwise(0)).as(s"${f}__M"),
        sum(when(col(f).isNull, 1).otherwise(0)).as(s"${f}__total"))
    }
    val longRow = matches.agg(longAggs.head, longAggs.tail: _*).first()
    val wideCols = weatherColNames()
    val wideAggs = wideCols.map(c => sum(when(col(c).isNull, 1).otherwise(0)).as(c))
    val wideRow  = jt.agg(wideAggs.head, wideAggs.tail: _*).first()
    println(f"${"attribut"}%-18s ${"hors_appariement"}%17s ${"valeur_M"}%10s ${"total_long"}%11s ${"total_large"}%12s  verdict")
    Features.foreach { f =>
      val unmatched = longRow.getAs[Long](s"${f}__unmatched")
      val nM        = longRow.getAs[Long](s"${f}__M")
      val total     = longRow.getAs[Long](s"${f}__total")
      val wide      = wideCols.filter(_.startsWith(s"${f}_")).map(c => wideRow.getAs[Long](c)).sum
      println(f"$f%-18s $unmatched%17d $nM%10d $total%11d $wide%12d  ${ok(total == unmatched + nM && total == wide)}")
    }

    println("\n--- [JOIN] Contrôle 7 : colonnes météo de JT (attendu 208) ---")
    val present = wideCols.count(c => jt.columns.contains(c))
    println(s"Colonnes météo présentes : $present / ${wideCols.size}   ${ok(present == 208)}")
    println("Aperçu S et D (k = 0) :")
    jt.select("SkyCondition_orig_0", "WeatherType_orig_0", "SkyCondition_dest_0", "WeatherType_dest_0")
      .show(5, truncate = false)
  }

  /** Vérification à la main : 13 heures demandées + bulletins retenus, pour 4 vols types. */
  def printTraces(ft: DataFrame, matches: DataFrame): Unit = {
    println("\n--- [JOIN] Contrôle 8 : vérification à la main ---")
    def trace(label: String, cond: Column): Unit = {
      val r = ft.filter(cond)
        .orderBy("FL_DATE", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM")
        .select(col("flight_id"), col("FL_DATE").cast("string"), col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID"),
          // formaté par Spark (session UTC) : Timestamp.toString de Java appliquerait le fuseau du Mac
          date_format(col("t_sd"), "yyyy-MM-dd HH:mm"), date_format(col("t_sa"), "yyyy-MM-dd HH:mm"))
        .limit(1).collect()
      if (r.isEmpty) println(s"\n[$label] aucun vol trouvé")
      else {
        val v = r(0)
        println(s"\n[$label] FL_DATE = ${v.get(1)}   ${v.get(2)} -> ${v.get(3)}   t_sd = ${v.get(4)}   t_sa = ${v.get(5)}")
        matches.filter(col("flight_id") === v.getLong(0))
          .select(col("side"), col("k"),
            from_unixtime(col("req_ts"), "yyyy-MM-dd HH:mm").as("heure_demandee"),
            from_unixtime(col("chosen_ts"), "yyyy-MM-dd HH:mm").as("bulletin_retenu"),
            col("gap_min").as("ecart_min"), col("status"), col("tie").as("egalite"),
            col("DryBulbFarenheit"), col("Visibility"), col("SkyCondition"), col("WeatherType"))
          .orderBy(col("side").desc, col("k"))      // orig puis dest
          .show(26, truncate = false)
      }
    }
    trace("ATL, départ 09:20–09:29, le 10",
      col("ORIGIN_AIRPORT_ID") === 10397 && col("CRS_DEP_TIME").between(920, 929) && dayofmonth(col("FL_DATE")) === 10)
    trace("vol de nuit (départ avant 03:00), le 10",
      col("CRS_DEP_TIME") < 300 && dayofmonth(col("FL_DATE")) === 10)
    trace("arrivée le lendemain (arr_day_offset = 1), le 10",
      col("arr_day_offset") === 1 && dayofmonth(col("FL_DATE")) === 10)
    trace("1er jour du fichier, départ avant 08:00 (D5)",
      dayofmonth(col("FL_DATE")) === 1 && col("CRS_DEP_TIME") < 800)
  }

  /**
   * Comparaison avec une jointure par heure arrondie, pour k = 0 seulement :
   * heure tronquée (09:23 -> 09:00) + premier bulletin de cette heure (= deduplicateHourly).
   * Montre l'effet du choix "bulletin le plus proche".
   * expectedOld = 1 684 vols sans météo (origine OU destination) avec la règle arrondie.
   */
  def printComparisonD10(matches: DataFrame, ot: DataFrame, expectedOld: Long = 1684L): Unit = {
    println("\n--- [JOIN] Contrôle 9 : comparaison avec l'ancienne jointure (k = 0) [D10] ---")
    val wHour = Window.partitionBy("o_airport", "o_hour").orderBy("o_ts")
    val oldObs = ot.select(col("AirportID").as("o_airport"), col("obs_time").cast("long").as("o_ts"),
        col("DryBulbFarenheit").as("o_temp"))
      .withColumn("o_hour", (floor(col("o_ts") / 3600) * 3600).cast("long"))
      .withColumn("rn", row_number().over(wHour))
      .filter(col("rn") === 1)
      .select(col("o_airport"), col("o_hour"), col("o_ts").as("old_ts"), col("o_temp").as("old_temp"))

    val k0 = matches.filter(col("k") === 0)
      .withColumn("req_hour", (floor(col("req_ts") / 3600) * 3600).cast("long"))
      .join(oldObs, col("AirportID") === col("o_airport") && col("req_hour") === col("o_hour"), "left")
      .cache()

    // Vols sans météo : règle arrondie (température k = 0 nulle, origine OU destination)
    val nOldMissing = k0.filter(col("old_temp").isNull).select("flight_id").distinct().count()
    val nNewMissing = k0.filter(col("DryBulbFarenheit").isNull).select("flight_id").distinct().count()
    println(f"Vols sans météo k = 0 (origine OU destination) : ancienne = $nOldMissing%d (attendu $expectedOld%d) ${ok(nOldMissing == expectedOld)}   nouvelle = $nNewMissing%d")

    val nK0      = k0.count()
    val changed  = k0.filter(!(col("old_ts") <=> col("chosen_ts")))
    val nChanged = changed.count()
    val nOldPost = changed.filter(col("old_ts") - col("req_ts") > 1800).count()
    val nOldNone = changed.filter(col("old_ts").isNull && col("chosen_ts").isNotNull).count()
    println(f"Heures k = 0 (vols × 2 côtés) : $nK0%d")
    println(f"  bulletin différent de l'ancien     : $nChanged%d (${nChanged * 100.0 / nK0}%.2f%%)")
    println(f"  dont ancien postérieur de > 30 min : $nOldPost%d")
    println(f"  dont ancien absent, nouveau trouvé : $nOldNone%d")
    k0.unpersist()
  }
}
