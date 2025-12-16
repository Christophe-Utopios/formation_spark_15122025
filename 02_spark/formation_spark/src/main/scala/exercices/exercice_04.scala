package exercices

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object exercice_04 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._

    val ventesData = Seq(
      ("CMD001", "Alice Martin", "2024-03-15", "Électronique", 1299.99, 1, "Premium", "alice.martin@email.com"),
      ("CMD002", "Bob Durand", "2024-03-16", "Vêtements", 89.50, 3, "Standard", "bob.durand@email.com"),
      ("CMD003", "Claire Dubois", "2024-03-17", "Maison", 45.00, 2, "Premium", "claire.dubois@email.com"),
      ("CMD004", "David Moreau", "2024-03-18", "Sport", 199.99, 1, "Standard", "david.moreau@email.com"),
      ("CMD005", "Emma Petit", "2024-03-19", "Électronique", 799.00, 2, "VIP", "emma.petit@email.com"),
      ("CMD006", "Frank Lambert", "2024-03-20", "Livres", 29.99, 5, "Standard", "frank.lambert@email.com"),
      ("CMD007", "Grace Bernard", "2024-03-21", "Beauté", 156.75, 1, "Premium", "grace.bernard@email.com"),
      ("CMD008", "Henri Rousseau", "2024-03-22", "Électronique", 2199.00, 1, "VIP", "henri.rousseau@email.com")
    )

    val df = ventesData.toDF("id_commande", "nom_client", "date_commande", "categorie", "prix_unitaire", "quantite", "statut_client", "email")

    // Exercice 1 : Classification des ventes
    val classifierVente = udf((prixUnitaire: Double) => {
      prixUnitaire match {
        case prix if prix < 50.0 => "Vente faible"
        case prix if prix >= 50.0 && prix < 200.0 => "Vente moyenne"
        case prix if prix >= 200.0 && prix < 1000.0 => "Vente élevée"
        case _ => "Vente premium"
      }
    })

    // Exercice 2 : Calcul du montant total avec remise
    val calculerMontantTotal = udf((prixUnitaire: Double, quantite: Int, statutClient: String) => {
      val montantBrut = prixUnitaire * quantite
      val tauxRemise = statutClient match {
        case "Standard" => 0.0    // Aucune remise
        case "Premium" => 0.05    // 5% de remise
        case "VIP" => 0.10        // 10% de remise
        case _ => 0.0
      }
      val montantFinal = montantBrut * (1.0 - tauxRemise)
      math.round(montantFinal * 100.0) / 100.0
    })

    // Exercice 3 : Score de fidélité client
    val calculerScoreFidelite = udf((statutClient: String, montantTotal: Double, categorie: String) => {
      val scoreBase = statutClient match {
        case "Standard" => 1
        case "Premium" => 2
        case "VIP" => 3
        case _ => 0
      }
      val bonusCategorie = categorie match {
        case "Électronique" => 2
        case "Sport" => 1
        case _ => 0
      }
      val bonusMontant = (montantTotal / 100.0).toInt
      scoreBase + bonusCategorie + bonusMontant
    })

    println("\nApplication de tous les exercices:")
    val dfFinal = df
      .withColumn("classifierVente", classifierVente(col("prix_unitaire")))
      .withColumn("calculerMontantTotal", calculerMontantTotal(col("prix_unitaire"), col("quantite"), col("statut_client")))
      .withColumn("calculerScoreFidelite", calculerScoreFidelite(col("statut_client"), col("calculerMontantTotal"), col("categorie")))

    dfFinal.show()
  }
}
