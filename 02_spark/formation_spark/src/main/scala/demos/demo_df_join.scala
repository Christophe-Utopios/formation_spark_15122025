package demos

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object demo_df_join {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo-df-join").getOrCreate()

    val livresSchema = StructType(Array(
      StructField("livre_id", StringType, nullable = false),
      StructField("titre", StringType, nullable = false),
      StructField("auteur_id", StringType, nullable = true),
    ))

    val livresData = Seq(
      Row("L1", "Titre 1", "A1"),
      Row("L2", "Titre 2", "A2"),
      Row("L3", "Titre 3", "A1"),
      Row("L4", "Titre 4", "A3"),
      Row("L5", "Titre 5", null),
    )

    val livresRDD = spark.sparkContext.parallelize(livresData)
    val dfLivres = spark.createDataFrame(livresRDD, livresSchema)

    // DF Auteur
    val auteurSchema = StructType(Array(
      StructField("auteur_id", StringType, nullable = false),
      StructField("nom", StringType, nullable = false)
    ))

    val auteurData = Seq(
      Row("A1", "Toto"),
      Row("A2", "Tata"),
      Row("A3", "Titi"),
      Row("A4", "Tutu"),
    )

    val auteurRDD = spark.sparkContext.parallelize(auteurData)
    val dfauteur = spark.createDataFrame(auteurRDD, auteurSchema)

    println("INNER JOIN")
    // INNER JOIN - Seulement les livres avec un auteur
    val innerDF = dfLivres.join(dfauteur, Seq("auteur_id"), "inner")

    innerDF.show()

    println("LEFT JOIN")
    // LEFT JOIN - Tous les livres et seulement les auteurs avec un livre
    val leftDF = dfLivres.join(dfauteur, dfLivres("auteur_id") === dfauteur("auteur_id"), "left")

    leftDF.show()

    println("ANTI JOIN")
    // ANTI JOIN - Tous les livres sans auteur
    val antiDF = dfLivres.join(dfauteur, Seq("auteur_id"), "anti")

    antiDF.show()
  }
}
