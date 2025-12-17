package exercices
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object correction_tp {
  def main(args: Array[String]): Unit = {
    // Initialisation de la SparkSession
    val spark = SparkSession.builder()
      .appName("Superstore Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // PARTIE 1 : CHARGEMENT ET EXPLORATION
    // 1. Charger le CSV en DataFrame avec l'option header
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")           // Caractère de guillemet
      .option("escape", "\"")          // Caractère d'échappement
      .option("multiLine", "true")     // Supporter les lignes multi-lignes
      .csv("./Data/Sample - Superstore.csv")

    // 2. Afficher le schéma du DataFrame
    df.printSchema()

    // 3. Afficher les 20 premières lignes
    df.show(20, truncate = false)

    // 4. Compter le nombre total de lignes
    val totalRows = df.count()
    println(s"Nombre total de lignes : $totalRows")

    // 5. Afficher les régions uniques
    println("\n--- Régions uniques ---")
    df.select("Region").distinct().show()

    // PARTIE 2 : TRANSFORMATIONS SIMPLES

    // 1. Créer une colonne Profit Margin = Profit / Sales
    val dfWithMargin = df.withColumn("Profit Margin",
      col("Profit").cast("double") / col("Sales").cast("double"))

    // 2. Créer une colonne Year en extrayant l'année de Order Date
    val dfWithYear = dfWithMargin.withColumn("Year",
      year(to_timestamp(col("Order Date"), "M/d/yyyy")))

    // 3. Créer une colonne Total Value = Sales - Discount
    val dfTransformed = dfWithYear.withColumn("Total Value",
      col("Sales").cast("double") - col("Discount").cast("double"))

    // 4. Afficher les 10 premières lignes avec ces nouvelles colonnes
    dfTransformed.select("Order ID", "Sales", "Profit", "Discount",
        "Profit Margin", "Year", "Total Value")
      .show(10)

    // 5. Mettre ce DataFrame en cache
    dfTransformed.cache()

    // PARTIE 3 : UDF - CATÉGORISATION DES VENTES

    // 1. Créer une UDF categorizeSale
    val categorizeSale = udf((sales: Double) => {
      if (sales < 100) "Petite vente"
      else if (sales >= 100 && sales <= 500) "Vente moyenne"
      else "Grosse vente"
    })

    // 2. Appliquer cette UDF pour créer une colonne Sale Category
    val dfWithSaleCategory = dfTransformed.withColumn("Sale Category", categorizeSale(col("Sales")))

    // 3. Afficher quelques lignes avec cette nouvelle colonne
    dfWithSaleCategory.select("Order ID", "Sales", "Sale Category").show()

    // 4. Compter le nombre de ventes par catégorie
    dfWithSaleCategory.groupBy("Sale Category")
      .count()
      .orderBy(desc("count"))
      .show()

    // PARTIE 4 : UDF - NIVEAU DE REMISE

    // 1. Créer une UDF discountLevel
    val discountLevel = udf((discount: Double) => {
      if (discount == 0) "Pas de remise"
      else if (discount > 0 && discount <= 0.2) "Remise faible"
      else "Remise forte"
    })

    // 2. Appliquer cette UDF pour créer une colonne Discount Level
    val dfWithDiscountLevel = dfWithSaleCategory.withColumn("Discount Level", discountLevel(col("Discount")))

    dfWithDiscountLevel.select("Order ID", "Discount", "Discount Level").show(15)

    // 3. Calculer le CA total par niveau de remise
    dfWithDiscountLevel.groupBy("Discount Level")
      .agg(
        sum("Sales").alias("CA Total"),
        count("*").alias("Nombre de ventes")
      )
      .orderBy(desc("CA Total"))
      .show()


    // PARTIE 5 : AGRÉGATIONS BASIQUES

    // 1. Calculer le CA total par région
    dfWithDiscountLevel.groupBy("Region")
      .agg(sum("Sales").alias("CA Total"))
      .orderBy(desc("CA Total"))
      .show()

    // 2. Calculer le profit total par catégorie de produit
    dfWithDiscountLevel.groupBy("Category")
      .agg(sum("Profit").alias("Profit Total"))
      .orderBy(desc("Profit Total"))
      .show()

    // 3. Calculer le nombre de commandes par segment client
    dfWithDiscountLevel.groupBy("Segment")
      .agg(countDistinct("Order ID").alias("Nombre de commandes"))
      .orderBy(desc("Nombre de commandes"))
      .show()

    // 4. Identifier les 10 produits les plus vendus en quantité
    dfWithDiscountLevel.groupBy("Product Name")
      .agg(sum("Quantity").alias("Quantité Totale"))
      .orderBy(desc("Quantité Totale"))
      .show()

    // 5. Identifier les 5 états avec le plus de CA
    println("\n--- Top 5 états avec le plus de CA ---")
    dfWithDiscountLevel.groupBy("State")
      .agg(sum("Sales").alias("CA Total"))
      .orderBy(desc("CA Total"))
      .show(5)


    // PARTIE 6 : BROADCAST VARIABLE - CODES RÉGION

    // 1. Créer une Map qui associe chaque région à un code
    val regionCodes = Map(
      "East" -> "EST",
      "West" -> "WST",
      "Central" -> "CTR",
      "South" -> "STH"
    )

    // 2. Broadcaster cette Map
    val broadcastRegionCodes = spark.sparkContext.broadcast(regionCodes)

    // 3. Créer une UDF qui utilise cette broadcast variable
    val getRegionCode = udf((region: String) => {
      broadcastRegionCodes.value.getOrElse(region, "UNKNOWN")
    })

    val dfWithRegionCode = dfWithDiscountLevel.withColumn("Region Code", getRegionCode(col("Region")))

    // 4. Afficher quelques lignes avec le code région
    dfWithRegionCode.select("Region", "Region Code", "Sales").show()

    // PARTIE 7 : BROADCAST VARIABLE - COEFFICIENTS DE PRIORITÉ

    // 1. Créer une Map de coefficients par catégorie
    val categoryPriority = Map(
      "Technology" -> 1.5,
      "Furniture" -> 1.2,
      "Office Supplies" -> 1.0
    )

    // 2. Broadcaster cette Map
    val broadcastCategoryPriority = spark.sparkContext.broadcast(categoryPriority)

    // 3. Créer une UDF qui multiplie le Profit par le coefficient
    val getWeightedProfit = udf((profit: Double, category: String) => {
      val coefficient = broadcastCategoryPriority.value.getOrElse(category, 1.0)
      profit * coefficient
    })

    val dfFinal = dfWithRegionCode.withColumn(
      "Weighted Profit",
      getWeightedProfit(col("Profit"), col("Category"))
    )

    dfFinal.select("Category", "Profit", "Weighted Profit").show()

    // 4. Calculer le weighted profit total par catégorie
    dfFinal.groupBy("Category")
      .agg(
        sum("Profit").alias("Profit Total"),
        sum("Weighted Profit").alias("Weighted Profit Total")
      )
      .orderBy(desc("Weighted Profit Total"))
      .show()

    // NETTOYAGE
    dfTransformed.unpersist()
  }
}
