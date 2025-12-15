package exercices

import org.apache.spark.sql.SparkSession

object exercice_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("exercice01").getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    // Lecture du fichier CSV
    val rddAchats = sc.textFile("./data/achats_clients.csv")

    // Affichage des 10 premières lignes
//    rddAchats.take(10).foreach(println)

    // Filtrage de la première ligne
    val firstline = rddAchats.first()
    val rdd = rddAchats.filter(ligne => ligne != firstline)

//    val header = rddAchats.first()
//    val data = rddAchats.filter(_ != header)

//    rdd.take(5).foreach(println)
    // Transformation : extraction de id_client et montant
    val rddClients = rdd.map(line => {
      val fields = line.split(",")
      (fields(0), fields(2).toDouble)
    })

//    rddClients.take(5).foreach(println)

    // Agrégation par client (somme des montants)
    val montantClients = rddClients.reduceByKey((a, b) => a + b)
    val result = montantClients.collect()

    result.foreach(println)

    // Affichage formaté
    result.foreach {
      case (idClient, montant) =>
        println(s"Client $idClient | montant : $montant €")
    }
  }

}
