package demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object demo_streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]") // 2 Threads MINIMUM pour le streaming
      .getOrCreate()

    import spark.implicits._

    // Connexion à un socket TCP localhost:9999
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

    // Traitement : découper en mots et compter
    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    // Afficher les résultats
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    println("Appuyez sur Ctrl+C pour arrêter")
    query.awaitTermination()
  }
}
