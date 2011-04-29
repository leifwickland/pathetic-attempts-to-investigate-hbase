import scala.actors.{Actor,Exit}
import scala.actors.Actor._

object A {
  type JsonObject = Map[Any,Any]
  def main(args: Array[String]) {
    val jsonObjectActor = actor {
      println("Top of actor")
      loop {
        println("Top of loop")
        receive {
          case json: JsonObject => println("Actor received: " + json)
          case Exit => exit()
          case _ => println("Actor got something unexpected")
        }
        println("Bottom of loop")
      }
      println("Bottom of actor")
    }
    println("1")
    jsonObjectActor ! Map(1 -> "one")
    println("2")
    jsonObjectActor ! Map(2 -> "two")
    println("3")
    jsonObjectActor ! Map(3 -> "three")
    println("4")
    jsonObjectActor ! Exit
    println("5")
  }
}
