package flight

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

/**
 * Flights_v2 — préparation des données et jointure (Belcastro et al. 2016, §2 et §4.1).
 *
 * Étapes :
 *   1. Vols (table FT) : exclusion des vols annulés et déroutés (§4.1) ; causes de retard
 *      vides -> 0 ; aéroports sans station retirés ; reconstruction de t_sa (CRS_ARR_TIME)
 *      avec fuseaux horaires et heure d'été US ; session Spark en UTC (fuseau neutre).
 *   2. Météo (table OT) : les 8 attributs de O = <A,t,T,H,Wd,Ws,P,S,V,D> (sans HourlyPrecip) ;
 *      conversion stricte des nombres (suffixes qualité, bornes physiques, aucun null
 *      silencieux) ; WeatherType vide = "aucun phénomène" -> "NONE" ; M -> null ;
 *      toutes les observations à leur heure EXACTE, seuls les doublons stricts sont retirés.
 *   3. Jointure (table JT) : pour chaque vol, 13 heures demandées (t_sd − k h au départ,
 *      t_sa − k h à l'arrivée, k = 0…12) ; bulletin le plus proche (avant ou après,
 *      antérieur si égalité, sans écart maximal) ; heures hors période chargée -> null ;
 *      208 colonnes météo (6 numériques + S + D).
 *
 * Données : un mois (2013-04) — data/flights/201304.csv + data/weather/201304hourly.txt
 * Sortie   : data/cached/flights_weather_joined_13obs_v2.parquet
 */
object Flights_v2 {
  def main(args: Array[String]): Unit = {

    // ----- Chemins : 1 seul fichier vols + 1 seul fichier météo -----
    // (on peut toujours les surcharger en passant 4 arguments)
    val flightsPath = if (args.length > 0) args(0) else "data/flights/201304.csv"
    val weatherPath = if (args.length > 1) args(1) else "data/weather/201304hourly.txt"
    val mappingPath = if (args.length > 2) args(2) else "data/wban_airport_timezone.csv"
    val outputPath  = if (args.length > 3) args(3) else "data/cached/flights_weather_joined_13obs_v2.parquet"

    val spark = SparkSession.builder()
      .appName("FlightDelayPredictor_v2")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      // Fuseau neutre : Spark ne doit appliquer AUCUNE règle d'heure d'été
      // (celle de Paris par défaut). Les heures restent des heures légales locales US,
      // on fait seulement de l'arithmétique d'horloge.
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // Nombre d'exécutor
    //val numExecutors = spark.sparkContext.statusTracker.getExecutorInfos.length
    //println(s"Nombre d'exécuteurs : $numExecutors")

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ----- Lecture des vols -----
    val flights = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightsPath)
      // .csv("data/flights/201304.csv")

    println(s"Nombre de vols : ${flights.count()}")
    println(s"Nombre de colonnes : ${flights.columns.length}")
    flights.printSchema()
    flights.show(5, truncate = false)

    // ----- Lecture de la météo -----
    val weather = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherPath)
      // .csv("data/weather/201304hourly.txt")

    println(s"Nombre d'observations météo : ${weather.count()}")
    weather.printSchema()
    weather.show(5, truncate = false)

    // Lecture météo en TEXTE (pas d'inférence de type) :
    // la conversion en nombre est faite par WeatherPipeline_v2, valeur par valeur
    val weatherStr = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(weatherPath)


    // ----- EDA dataset vols -----
    println("\n" + "="*70)
    println(" EDA DATASET VOLS ")
    println("="*70)

    // Drop la colonne fantôme _c12
    val flightsClean = flights.drop("_c12")

    // 1. Comptage des NULL par colonne
    println("\n--- Nombre de NULL par colonne ---")
    val totalFlights = flightsClean.count()
    val nullCounts = flightsClean.columns.map { c =>
      val nullCount = flightsClean.filter(col(c).isNull).count()
      val pct = (nullCount.toDouble / totalFlights * 100)
      (c, nullCount, f"$pct%.2f%%")
    }
    nullCounts.foreach { case (c, n, p) =>
      println(f"$c%-25s $n%10d  ($p)")
    }

    // 2. Statistiques numériques (describe)
    println("\n--- Statistiques descriptives ---")
    flightsClean.describe(
      "ARR_DELAY_NEW", "CRS_DEP_TIME", "CRS_ELAPSED_TIME",
      "WEATHER_DELAY", "NAS_DELAY", "CANCELLED", "DIVERTED"
    ).show()

    // 3. Distribution des classes (seuils 15, 30, 60 min)
    println("\n--- Distribution on-time / delayed selon le seuil ---")
    val operational = flightsClean.filter($"CANCELLED" === 0 && $"DIVERTED" === 0)
    val nOperational = operational.count()
    println(s"Vols opérationnels (non cancelled, non diverted) : $nOperational")

    Seq(15, 30, 60).foreach { th =>
      val delayed = operational.filter($"ARR_DELAY_NEW" >= th).count()
      val pct = delayed.toDouble / nOperational * 100
      println(f"Seuil $th min : delayed = $delayed%d  (${pct}%.2f%%)")
    }

    // 4. Vols avec retard météo identifié
    println("\n--- Vols avec causes de retard identifiées ---")
    val withWeather = operational.filter($"WEATHER_DELAY" > 0).count()
    val withNAS = operational.filter($"NAS_DELAY" > 0).count()
    println(f"Vols avec WEATHER_DELAY > 0 : $withWeather (${withWeather.toDouble/nOperational*100}%.2f%%)")
    println(f"Vols avec NAS_DELAY > 0     : $withNAS (${withNAS.toDouble/nOperational*100}%.2f%%)")

    // 5. Cardinalités
    println("\n--- Cardinalités ---")
    println(s"Aéroports d'origine uniques     : ${operational.select("ORIGIN_AIRPORT_ID").distinct().count()}")
    println(s"Aéroports de destination uniques: ${operational.select("DEST_AIRPORT_ID").distinct().count()}")
    println(s"Compagnies aériennes uniques    : ${operational.select("OP_CARRIER_AIRLINE_ID").distinct().count()}")


    // ----- EDA dataset Météo -----
    println("\n" + "="*70)
    println(" EDA DATASET MÉTÉO ")
    println("="*70)

    // 1. Stations uniques
    println(s"\nNombre de stations WBAN uniques : ${weather.select("WBAN").distinct().count()}")
    println(s"Plage de dates : ")
    weather.agg(min("Date"), max("Date")).show()

    // 2. Comptage des M (missing) et NULL pour les colonnes clés
    println("\n--- Valeurs manquantes (M ou NULL) pour colonnes clés ---")
    val totalWeather = weather.count()
    val keyCols = Seq(
      "DryBulbFarenheit", "WindSpeed", "WindDirection",
      "RelativeHumidity", "StationPressure", "Visibility",
      "SkyCondition", "WeatherType", "HourlyPrecip"
    )
    keyCols.foreach { c =>
      val missing = weather.filter(col(c).isNull || col(c) === "M" || col(c) === "").count()
      val pct = missing.toDouble / totalWeather * 100
      println(f"$c%-20s missing = $missing%10d  (${pct}%.2f%%)")
    }


    // ----- EDA fichier mapping -----
    println("\n" + "="*70)
    println(" EDA FICHIER MAPPING AIRPORT-WBAN ")
    println("="*70)

    val mapping = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mappingPath)
      // .csv("data/wban_airport_timezone.csv")

    mapping.printSchema()
    println(s"Nombre de mappings : ${mapping.count()}")
    mapping.show(5, truncate = false)

    // Vérification : tous les airport IDs des vols sont-ils dans le mapping ?
    val flightAirports = flightsClean.select($"ORIGIN_AIRPORT_ID".as("AirportID"))
      .union(flightsClean.select($"DEST_AIRPORT_ID".as("AirportID")))
      .distinct()

    val missingMapping = flightAirports.join(mapping, Seq("AirportID"), "left_anti").count()
    println(s"Aéroports présents dans les vols mais ABSENTS du mapping : $missingMapping")

    // Récupérer la liste des aéroports orphelins (pour le filtrage en preprocessing)
    // left_anti:retourne uniquement les lignes du flightAirports qui n'ont PAS de correspondance à mapping
    val orphanAirports = flightAirports.join(mapping, Seq("AirportID"), "left_anti")

    // Combien de vols touchés (origine OU destination orpheline)
    val touchedFlights = flightsClean
      .join(orphanAirports,
        $"ORIGIN_AIRPORT_ID" === $"AirportID" || $"DEST_AIRPORT_ID" === $"AirportID",
        "left_semi")
      .count()
    println(s"Vols touchés par un aéroport orphelin : $touchedFlights " +
      s"(${touchedFlights.toDouble/totalFlights*100}%)")

    // Voir lesquels manquent
    orphanAirports.show(50, truncate = false)

    // ============================================================
    // PREPROCESSING
    // ============================================================

    println("\n" + "="*70)
    println(" PREPROCESSING DATASET VOLS ")
    println("="*70)

    // Étape 1: NULL → 0 pour WEATHER_DELAY et NAS_DELAY (79% NULL = pas de retard de cette cause)
    // NAS = National Airspace System
    // WEATHER_DELAY et NAS_DELAY sont les minutes de retard à l'arrivée imputables respectivement à la météo extrême et
    // au système de contrôle aérien américain, parmi cinq catégories de causes obligatoirement déclarées par les compagnies
    // pour les vols significativement retardés. Les null (79 % des cas) ne sont pas des données manquantes mais une
    // convention de stockage signifiant "pas de retard de cette cause" — donc on les remplace par 0.0 pour rétablir la
    // valeur sémantique correcte et permettre des calculs arithmétiques propres en aval, sans risquer la propagation de null
    // qui casserait toute feature dérivée.
    val flightsFilled = flightsClean.na.fill(0.0, Seq("WEATHER_DELAY", "NAS_DELAY"))

    // Étape 2 : exclure explicitement les vols annulés et déroutés
    // (Belcastro et al. 2016, §4.1 : "we filtered out diverted and canceled flights")
    // Un vol annulé n'a pas d'arrivée ; un vol dérouté n'arrive pas à A_d :
    // dans les deux cas AD(F) = t_aa - t_sa n'est pas défini.
    val flightsOperated = flightsFilled.filter($"CANCELLED" === 0.0 && $"DIVERTED" === 0.0)

    // Étape 2 bis : contrôle de sécurité — un vol opéré sans retard renseigné
    // n'a pas d'étiquette, il est inutilisable (attendu : 0 ligne)
    val flightsLabeled = flightsOperated.filter($"ARR_DELAY_NEW".isNotNull)

    // Vérif : détail des exclusions en une seule passe sur les données
    val nBefore = flightsClean.count()
    val stats = flightsFilled.agg(
      sum(when($"CANCELLED" === 1.0, 1).otherwise(0)).as("cancelled"),
      sum(when($"CANCELLED" =!= 1.0 && $"DIVERTED" === 1.0, 1).otherwise(0)).as("diverted"),
      sum(when($"CANCELLED" === 0.0 && $"DIVERTED" === 0.0 && $"ARR_DELAY_NEW".isNull, 1)
        .otherwise(0)).as("no_label")
    ).first()
    val nAfter = flightsLabeled.count()

    println(f"Avant                        : $nBefore%d")
    println(f"  - vols annulés             : ${stats.getLong(0)}%d")
    println(f"  - vols déroutés            : ${stats.getLong(1)}%d")
    println(f"  - vols opérés sans retard  : ${stats.getLong(2)}%d")
    println(f"Après (FT)                   : $nAfter%d  (${nAfter.toDouble/nBefore*100}%.2f%% conservés)")

    // Étape 3: filtrer les aéroports orphelins (déjà identifiés dans l'EDA)
    val orphans = orphanAirports.select($"AirportID".as("OrphanID"))

    val flightsMapped = flightsLabeled
      .join(orphans, $"ORIGIN_AIRPORT_ID" === $"OrphanID", "left_anti")
      .join(orphans, $"DEST_AIRPORT_ID" === $"OrphanID", "left_anti")

    val nAfterMapping = flightsMapped.count()
    println(f"Étape 3:    $nAfter%d → $nAfterMapping%d (perdus: ${nAfter - nAfterMapping}%d)")

    println(f"\n>> Dataset final pour la suite : $nAfterMapping%d vols (${nAfterMapping.toDouble/nBefore*100}%.2f%% des données brutes)")


    // ============================================================
    // PREPROCESSING - SUITE
    // ============================================================

    println("\n" + "=" * 70)
    println(" CRÉATION LABEL + FEATURES TEMPORELLES ")
    println("=" * 70)

    // Étape 4 : créer le label
    val delayThreshold = 15
    val flightsWithLabel = flightsMapped.withColumn(
      "is_delayed",
      when($"ARR_DELAY_NEW" >= delayThreshold, 1.0).otherwise(0.0)
    )

    // ============================================================
    // Étape 5 : reconstruire t_sa (CRS_ARR_TIME)
    // ------------------------------------------------------------
    // L'article dispose de t_sa (CRS_ARR_TIME = heure d'arrivée prévue, en heure
    // légale locale de l'aéroport d'arrivée). Nos fichiers ne l'ont pas : on la
    // reconstruit telle qu'elle figurerait sur le billet :
    //   t_sa = t_sd + durée + (TZ_dest - TZ_orig) + correction heure d'été
    // ============================================================

    // 5.1 Fuseau de chaque aéroport : la table WBAN est jointe DEUX fois
    //     (une fois pour l'origine, une fois pour la destination)
    val tzOrig = broadcast(mapping.select($"AirportID".as("ORIG_TZ_ID"), $"TimeZone".as("TZ_orig")))
    val tzDest = broadcast(mapping.select($"AirportID".as("DEST_TZ_ID"), $"TimeZone".as("TZ_dest")))

    val flightsWithTz = flightsWithLabel
      .join(tzOrig, $"ORIGIN_AIRPORT_ID" === $"ORIG_TZ_ID", "inner")
      .join(tzDest, $"DEST_AIRPORT_ID"   === $"DEST_TZ_ID", "inner")
      .drop("ORIG_TZ_ID", "DEST_TZ_ID")

    // 5.2 Heure d'été américaine (règle en vigueur depuis 2007) :
    //     du 2e dimanche de mars au 1er dimanche de novembre.
    //     Calculée à partir de l'année de chaque vol -> valable pour tous les fichiers.
    //     Le jour du changement est classé dans la nouvelle heure (le changement a lieu à 2h).
    val dstStart = next_day(make_date(year($"FL_DATE"), lit(3), lit(7)), "Sun")    // 2e dimanche de mars
    val dstEnd   = next_day(make_date(year($"FL_DATE"), lit(10), lit(31)), "Sun")  // 1er dimanche de novembre
    val isDstDate = $"FL_DATE" >= dstStart && $"FL_DATE" < dstEnd

    // Aéroports SANS heure d'été : Arizona (liste fixée), Hawaï (TZ -10),
    // Porto Rico / Îles Vierges (TZ -4)
    val arizonaAirports = Seq(14107, 15376, 11695, 16218)  // PHX, TUS, FLG, YUM
    def observesDst(airportId: Column, tz: Column): Column =
      !(airportId.isin(arizonaAirports: _*) || tz === -10 || tz === -4)

    // 5.3 Calcul de t_sd et t_sa (horodatages complets)
    val flightsWithArrTime = flightsWithTz
      // Heure d'été appliquée ou non, à chaque aéroport, à la date du vol (0 ou 1)
      .withColumn("dst_orig", when(isDstDate && observesDst($"ORIGIN_AIRPORT_ID", $"TZ_orig"), 1).otherwise(0))
      .withColumn("dst_dest", when(isDstDate && observesDst($"DEST_AIRPORT_ID",   $"TZ_dest"), 1).otherwise(0))
      // Correction ±1 h : non nulle seulement pour les vols "mixtes" en période d'heure d'été
      .withColumn("dst_correction_h", $"dst_dest" - $"dst_orig")
      // Différence d'heure entre les deux villes (heures) = (TZ_dest - TZ_orig) + correction
      .withColumn("tz_diff_h", ($"TZ_dest" - $"TZ_orig") + $"dst_correction_h")
      // t_sd : FL_DATE + CRS_DEP_TIME (HHMM -> minutes depuis minuit ; "2400" donne minuit du lendemain)
      .withColumn("dep_min_of_day",
        ($"CRS_DEP_TIME" / 100).cast("int") * 60 + ($"CRS_DEP_TIME" % 100))
      .withColumn("t_sd",
        ($"FL_DATE".cast("timestamp").cast("long") + $"dep_min_of_day".cast("long") * 60L)
          .cast("timestamp"))
      // t_sa = t_sd + durée + différence d'heure entre les deux villes
      .withColumn("t_sa",
        ($"t_sd".cast("long")
          + ($"CRS_ELAPSED_TIME" * 60).cast("long")
          + $"tz_diff_h".cast("long") * 3600L)
          .cast("timestamp"))
      // Décalage de jour déduit automatiquement des dates (0, 1, ou -1 dans de rares cas)
      .withColumn("arr_day_offset", datediff(to_date($"t_sa"), $"FL_DATE"))
      // CRS_ARR_TIME au format HHMM, en heure légale locale de l'aéroport d'arrivée
      .withColumn("CRS_ARR_TIME", hour($"t_sa") * 100 + minute($"t_sa"))

    // ---------- Contrôles de la reconstruction de t_sa ----------
    println("\n--- Contrôles A2 (reconstruction de t_sa) ---")

    // Contrôle 1 : aucun vol perdu à la jointure des fuseaux
    val nBeforeTz = flightsWithLabel.count()
    val nAfterTz  = flightsWithArrTime.count()
    println(s"Vols avant/après jointure fuseaux : $nBeforeTz / $nAfterTz (perdus : ${nBeforeTz - nAfterTz})")

    // Contrôle 2 : cohérence t_sa - t_sd = durée + différence d'heure (doit donner 0 incohérence)
    val nIncoherent = flightsWithArrTime.filter(
      $"t_sa".cast("long") - $"t_sd".cast("long") =!=
        ($"CRS_ELAPSED_TIME" * 60).cast("long") + $"tz_diff_h".cast("long") * 3600L
    ).count()
    println(s"Vols incohérents (t_sa - t_sd ≠ durée + décalage) : $nIncoherent")

    // Contrôle 3 : combien de vols reçoivent la correction d'heure d'été (attendu en avril : ~8,25 %)
    println("Répartition de la correction d'heure d'été (h) :")
    flightsWithArrTime.groupBy("dst_correction_h").count().orderBy("dst_correction_h").show()

    // Contrôle 4 : répartition de la différence d'heure entre villes
    println("Répartition de la différence d'heure origine -> destination (h) :")
    flightsWithArrTime.groupBy("tz_diff_h").count().orderBy("tz_diff_h").show()

    // Contrôle 5 : exemples à vérifier à la main
    //      JFK (12478) -> LAX (12892) : attendu t_sa = t_sd + durée - 3 h
    //      PHX (14107) -> LAX (12892) : attendu t_sa = t_sd + durée + 0 h en avril
    println("Exemples JFK -> LAX et PHX -> LAX :")
    flightsWithArrTime
      .filter(($"ORIGIN_AIRPORT_ID" === 12478 || $"ORIGIN_AIRPORT_ID" === 14107) && $"DEST_AIRPORT_ID" === 12892)
      .select("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "TZ_orig", "TZ_dest",
              "dst_orig", "dst_dest", "tz_diff_h", "CRS_ELAPSED_TIME", "t_sd", "t_sa",
              "CRS_ARR_TIME", "arr_day_offset")
      .orderBy("ORIGIN_AIRPORT_ID", "t_sd")
      .show(6, truncate = false)

    // Étape 6 : jour de semaine
    val flightsReady = flightsWithArrTime.withColumn(
      "day_of_week", dayofweek($"FL_DATE")
    )

    // Vérification visuelle
    println("\nExemples de transformation :")
    flightsReady.select(
      "FL_DATE", "day_of_week",
      "CRS_DEP_TIME", "CRS_ELAPSED_TIME", "CRS_ARR_TIME", "arr_day_offset",
      "ARR_DELAY_NEW", "is_delayed"
    ).show(10, truncate = false)

    // Distribution du label
    println("Distribution du label is_delayed :")
    flightsReady.groupBy("is_delayed").count().show()

    // Vols qui débordent sur le lendemain
    val overnight = flightsReady.filter($"arr_day_offset" >= 1).count()
    println(s"Vols atterrissant le lendemain (arr_day_offset >= 1) : $overnight")


    // ============================================================
    // EXPLORATION MÉTÉO — étapes de base (WeatherPipeline), pour référence
    // ============================================================
    // Étape 1 : stations d'aéroport
    println(s"Météo brute : ${weather.count()} lignes")

    val w1 = weather.join(mapping, Seq("WBAN"), "inner")
    println(s"Météo aéroports : ${w1.count()} lignes")
    println(f"Réduction: ${(1 - w1.count().toDouble/weather.count())*100}%.2f%%")

    println("\nExemples après jointure :")
    w1.select("WBAN", "AirportID", "TimeZone", "Date", "Time", "DryBulbFarenheit").show(5, truncate = false)

    // Étape 2 : colonnes utiles
    val w2 = w1.select("AirportID", "Date", "Time",
      "DryBulbFarenheit", "RelativeHumidity", "WindDirection",
      "WindSpeed", "StationPressure", "Visibility",
      "SkyCondition", "WeatherType", "HourlyPrecip")
    println(s"Après étape 2 : ${w2.count()} lignes, ${w2.columns.length} colonnes")
    w2.printSchema()
    w2.show(5, truncate = false)

    // Étape 3 : conversion simple
    val w3 = WeatherPipeline.cleanAndCast(w2)
    println("Schéma après cast :")
    w3.printSchema()
    println("\nExemples :")
    w3.show(10, truncate = false)

    // Stats : combien de nulls par colonne après cast ?
    println("\n--- NULL après cast ---")
    val totalW3 = w3.count()
    Seq("DryBulbFarenheit", "RelativeHumidity", "WindSpeed").foreach { c =>
      val nNull = w3.filter(col(c).isNull).count()
      val pct = nNull.toDouble / totalW3 * 100
      println(f"$c%-20s : $nNull%d nulls (${pct}%.2f%%)")
    }

    // Étape 4 : horodatage
    val w4 = WeatherPipeline.buildTimestamp(w3)
    println("Schéma après buildTimestamp :")
    w4.printSchema()
    println("\nExemples :")
    w4.select("AirportID", "obs_time", "DryBulbFarenheit", "WindSpeed").show(10, truncate = false)

    // Vérifier les bornes temporelles
    println("\nBornes temporelles :")
    w4.agg(min("obs_time"), max("obs_time")).show(truncate = false)

    // Étape 5 : une observation par heure
    val w5 = WeatherPipeline.deduplicateHourly(w4)
    println("Schéma final :")
    w5.printSchema()

    println(s"\nNombre d'observations avant dedup : ${w4.count()}")
    println(s"Nombre d'observations après dedup : ${w5.count()}")

    println("\nExemples (notez les heures pile) :")
    w5.select("AirportID", "obs_time", "DryBulbFarenheit", "WindSpeed")
      .orderBy("AirportID", "obs_time")
      .show(15, truncate = false)

    // Vérification : 1 seule obs par (aéroport, heure) ?
    println("\nVérification unicité (AirportID, obs_time) :")
    val nDistinct = w5.select("AirportID", "obs_time").distinct().count()
    val nTotal = w5.count()
    println(s"  Distinct : $nDistinct")
    println(s"  Total    : $nTotal")
    println(s"  Doublons : ${nTotal - nDistinct}")

    // ============================================================
    // MÉTÉO — attributs de O et conversion stricte des valeurs
    // ============================================================
    println("\n" + "=" * 70)
    println(" MÉTÉO v2 — B2 / B3 ")
    println("=" * 70)

    // Valeurs à suffixe qualité : Seq.empty = on garde la valeur numérique ;
    // Seq("s") = les valeurs "s" sont mises à null. Avril 2013 : aucune valeur à suffixe.
    val DropSuffixes: Seq[String] = Seq.empty

    val w1v2 = WeatherPipeline_v2.joinWithMapping(weatherStr, mapping)
    val w2v2 = WeatherPipeline_v2.selectKeyColumns(w1v2).cache()
    println(s"Colonnes OT (B2) : ${w2v2.columns.mkString(", ")}")

    // Profilage des valeurs brutes (avant conversion)
    WeatherPipeline_v2.printProfile(w2v2)

    // Conversion + contrôles (comparaison avec w3 = conversion simple)
    val w3v2diag = WeatherPipeline_v2.cleanAndCastWithDiagnostics(w2v2, DropSuffixes).cache()
    WeatherPipeline_v2.printControlsB3(w3v2diag, w3)

    // Profilage de S et D, puis contrôle WeatherType vide -> NONE
    WeatherPipeline_v2.printProfileCategorical(w2v2)
    WeatherPipeline_v2.printControlsB4(w2v2, w3v2diag)

    // ============================================================
    // MÉTÉO — table OT (heure exacte)
    // ============================================================
    val weatherOT = WeatherPipeline_v2.preprocess(weatherStr, mapping, DropSuffixes).cache()
    val w4v2 = WeatherPipeline.buildTimestamp(WeatherPipeline_v2.cleanAndCast(w2v2, DropSuffixes))
    WeatherPipeline_v2.printControlsB6(w4v2, weatherOT, mapping)

    // ============================================================
    // JOINTURE — FT × OT -> JT (§4.1)
    // ============================================================
    println("\n" + "=" * 70)
    println(" JOINTURE v2 — FT × OT -> JT (13 observations origine + destination) ")
    println("=" * 70)

    // Identifiant technique de vol (jamais sauvegardé). Persisté : il doit rester
    // identique entre les heures demandées et la jointure finale (contrôlé).
    val flightsFT = flightsReady
      .withColumn("flight_id", monotonically_increasing_id())
      .persist(StorageLevel.MEMORY_AND_DISK)
    val nFlightsFT = flightsFT.count()
    println(s"Vols FT : $nFlightsFT")

    // Période couverte par la météo chargée : [début, fin[
    val (periodStart, periodEnd) = JoinPipeline_v2.weatherPeriod(weatherOT)
    println(s"Période météo chargée : [${JoinPipeline_v2.fmt(periodStart)}, ${JoinPipeline_v2.fmt(periodEnd)}[")

    // 13 heures demandées au départ (t_sd − k h) + 13 à l'arrivée (t_sa − k h)
    val requests = JoinPipeline_v2.requestedTimes(flightsFT)

    // Bulletin le plus proche de chaque heure demandée, puis ses 8 attributs
    val matches = JoinPipeline_v2.attachObservations(
        JoinPipeline_v2.matchNearest(requests, weatherOT, periodStart, periodEnd),
        weatherOT)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // JT : une ligne par vol, 208 colonnes météo {attribut}_{orig|dest}_{k}
    val jt = JoinPipeline_v2.joinFlightsWeather(flightsFT, matches)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Contrôles
    JoinPipeline_v2.printControlsJoin(flightsFT, matches, jt)
    JoinPipeline_v2.printTraces(flightsFT, matches)
    JoinPipeline_v2.printComparisonD10(matches, weatherOT)

    // Sauvegarde : colonnes du vol + 208 colonnes météo (sans flight_id ni colonnes de contrôle)
    println("\nSauvegarde en cours...")
    val jtToSave = jt.drop("flight_id")
    jtToSave.write.mode("overwrite").parquet(outputPath)
    println(s"JT sauvegardée dans $outputPath (${jtToSave.columns.length} colonnes)")


    spark.stop()
  }
}
