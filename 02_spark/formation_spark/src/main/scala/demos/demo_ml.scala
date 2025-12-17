package demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

object demo_ml {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo-ML").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (50.0, 1.0, 10.0, 150.0),
      (75.0, 2.0, 8.0, 200.0),
      (100.0, 3.0, 5.0, 280.0),
      (12.0, 3.0, 3.0, 350.0),
      (85.0, 2.0, 3.0, 220.0),
      (95.0, 3.0, 2.0, 250.0),
      (60.0, 2.0, 3.0, 170.0),
      (110.0, 3.0, 2.0, 320.0),
      (80.0, 2.0, 4.0, 210.0),
      (130.0, 4.0, 9.0, 400.0)
    ).toDF("surface", "chambres", "distance_centre", "prix")

    // 1. Préparation des features avec VectorAssembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("surface", "chambres", "distance_centre"))
      .setOutputCol("features")

    val dataWithFeatures = assembler.transform(data)

    // 2. Split train/test (80/20)
    val Array(trainingData, testData) = dataWithFeatures.randomSplit(Array(0.8, 0.2), seed = 42)

    // 3. Création du modèle
    val lr = new LinearRegression()
      .setLabelCol("prix")
      .setFeaturesCol("features")
      .setMaxIter(100)

    val model = lr.fit(trainingData)

    val predictions = model.transform(testData)

    println("Prédictions vs valeurs réelles")
    predictions.select("surface", "chambres", "distance_centre", "prix", "prediction")
      .withColumn("erreur", $"prediction" - $"prix").show()

    // Prédictipon sur une nouvelle donnée
    val newData = Seq(
      (90.0, 2.0, 5)
    ).toDF("surface", "chambres", "distance_centre")

    val newDataFeatures = assembler.transform(newData)
    val newPrediction = model.transform(newDataFeatures)

    println("Valeur de la maison :")
    newPrediction.select("prediction").show()
  }
}
