package demos

import org.apache.spark.sql.SparkSession

object demo_RDD_film {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().master("local").appName("demo-film").getOrCreate()

      val sc = spark.sparkContext

      sc.setLogLevel("WARN")

      // Lecture du fichier film.data
      val rddFilm = sc.textFile("./data/film.data")

      // Récupération de la première ligne
      val firstLine = rddFilm.first()
    // Filtrage de la première ligne
      val rdd = rddFilm.filter(ligne => ligne != firstLine)

      // Extraction de la valeur qui nous intéresse (3éme colonne)
      val rddNotes = rddFilm.map(line => line.split("\t")(2))

      // On compte le nombre d'éléments pas valeur
      val result = rddNotes.countByValue()

      // Affichage du resultat
      result.toSeq.foreach{
        case  (key, value) =>
          println(s"$key = $value")
      }
  }
}
