package demos


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {
    // 1. Déclaration des variables
    val immutable = "ne change pas"
    var mutable = "peut changer"

    // 2. Collections
    var liste = List(1,2,3,4,5,6)
    var tableau = Array("a", "b", "c")
    var map = Map("prénom" -> "Toto", "age" -> 18)

    // 3. Fonctions anonymes (lambdas)
    var addition = (a: Int, b : Int) => a + b

    def maFonction(a : Int, b : Int ) : Int = {
      return a + b
    }

    println(mutable)
  }
}

