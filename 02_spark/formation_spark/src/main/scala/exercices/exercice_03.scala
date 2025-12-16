package exercices

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, desc, round}

object exercice_03 {
  def main(args: Array[String]): Unit = {
    // Création de la SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .appName("Movies Analysis")
      .getOrCreate()

    import spark.implicits._

    // Lecture des fichiers CSV
    val moviesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("./Data/movie.csv")

    val ratingsDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("./Data/rating.csv")

    // Affichage du nombre d'enregistrements
    println(moviesDf.count())
    println(ratingsDf.count())

    // INNER JOIN - Joindre les ratings avec les films
    val df = ratingsDf.join(moviesDf, Seq("movieId"), "inner")
    df.show()

    // Calculer le nombre de notes et la moyenne des notes par film
    // Trier par nombre de notes décroissant
    ratingsDf.groupBy("movieId")
      .agg(
        count("rating").alias("nb_rating"),
        round(avg("rating"), 2).alias("avg_rating")
      )
      .join(moviesDf, Seq("movieId"), "inner")
      .orderBy(desc("nb_rating"))
      .show()

    // LEFT SEMI JOIN - Films qui ont au moins une note
    println("LEFT SEMI JOIN - Films qui ont au moins une note")
    ratingsDf.join(moviesDf, Seq("movieId"), "left_semi").show()

    // LEFT ANTI JOIN - Films sans aucune note
    println("LEFT ANTI JOIN - Films sans aucune note")
    moviesDf.join(ratingsDf, Seq("movieId"), "left_anti").show()

    // LEFT OUTER JOIN - Liste complète des films avec stats si disponibles
    moviesDf.join(ratingsDf, Seq("movieId"), "left_outer")
      .groupBy("rating")
      .agg(
        count("rating").alias("rating_count"),
        round(avg("rating"), 2).alias("rating_avg")
      )
      .orderBy("rating")
      .show()

    // FULL OUTER JOIN - Diagnostic complet
    val diagnostic = moviesDf.join(ratingsDf, Seq("movieId"), "full")
    diagnostic.show()

    // A. Top 5 global - Films les mieux notés
    ratingsDf.groupBy("movieId")
      .agg(
        count("rating").alias("rating_nb"),
        round(avg("rating"), 2).alias("avg_rating")
      )
      .filter(col("rating_nb") >= 50)
      .join(moviesDf, Seq("movieId"), "inner")
      .orderBy(desc("avg_rating"))
      .show(5)
  }
}
