package demos

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object demo_broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("demo-broadcast").getOrCreate()

    import spark.implicits._

    val countryDF = Seq(
      ("US", "United States"),
      ("FR", "France"),
      ("De", "Germany"),
      ("JP", "Japan")
    ).toDF("country_code", "country_name")

    val userDf = Seq(
      ("Toto", "US", 30),
      ("Tata", "DE", 20),
      ("Titi", "FR", 25),
      ("Tutu", "JP", 30),
      ("Tutu", "AA", 30),
    ).toDF("name", "country_code", "age")

    // Broadcast join (Spark optimise automatiquement les petites tables
    val df = userDf.join(broadcast(countryDF), Seq("country_code"))

    // Broadcast de variable de config
    case class FilterConfig(
                           minAge : Int,
                           allowedCountries: Set[String]
                           )

    val config = FilterConfig(
      minAge = 25, allowedCountries = Set("FR", "US")
    )

    val broadcastConfig = spark.sparkContext.broadcast(config)

    val userFilterUDF = udf((age: Int, country: String) => {
      val config = broadcastConfig.value
      age >= config.minAge && config.allowedCountries.contains(country)
    })

    val filterDF = df.filter(userFilterUDF(col("age"), col("country_code")))

    filterDF.show()

    // Création d'un map
    val countryMapping = Map(
      "US" -> "United States",
      "FR" -> "France",
      "DE" -> "Germany",
      "JP" -> "Japan"
    )

    val broadcastCountries : Broadcast[Map[String,String]] = spark.sparkContext.broadcast(countryMapping)

    val getCountryUdf = udf((code: String) => {
      broadcastCountries.value.getOrElse(code, "Unknown")
    })

    val userDfCode = userDf.withColumn("country_name", getCountryUdf(col("country_code")))

    userDfCode.show()
    broadcastCountries.destroy()
    broadcastConfig.destroy()
  }
}
