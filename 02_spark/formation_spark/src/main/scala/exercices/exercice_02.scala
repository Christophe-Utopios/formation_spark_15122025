package exercices

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min}

object exercice_02 {
  val spark = SparkSession.builder.master("local").getOrCreate()

  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("./Data/housing.csv")

  // 1. Calculer min, max, moyenne pour median_house_value
  df.select(
    min("median_house_value").alias("min_value"),
    max("median_house_value").alias("max_value"),
    avg("median_house_value").alias("avg_value")
  ).show()

  // 2. Calculer min, max, moyenne pour median_income
  df.select(
    min("median_income").alias("min_value"),
    max("median_income").alias("max_value"),
    avg("median_income").alias("avg_value")
  ).show()

  // 3. Calculer min, max, moyenne pour housing_median_age
  df.select(
    min("housing_median_age").alias("min_value"),
    max("housing_median_age").alias("max_value"),
    avg("housing_median_age").alias("avg_value")
  ).show()

  // 4. Compter combien de districts ont une population > 5000
  val count = df.filter(col("population") > 5000).count()
  println(s"Nombre de districts avec population > 5000: $count")

  // Statistiques par type de proximité océan
  df.groupBy("ocean_proximity")
    .agg(
      avg("median_house_value").alias("prix_moyen"),
      avg("median_income").alias("revenu_moyen"),
      avg("housing_median_age").alias("age_moyen"),
      avg("population").alias("population_moyenne")
    )
    .orderBy(col("prix_moyen").desc)
    .show()
}
