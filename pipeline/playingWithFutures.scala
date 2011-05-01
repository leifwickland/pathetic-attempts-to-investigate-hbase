import scala.actors.Actor
import scala.actors.Actor._

object playingWithFutures {
  def main(args: Array[String]) {
    val incrementor = actor {
      receive {
        case i : Integer => reply(i + 1)
          case x : Any => println("Got something unexpected: " + x);
      }
    }

    println("Starting synchronous");
    for (i <- 1 until 10) {
      printf("i=%d  incr=%d\n", i, incrementor !? i)
    }
    println("Starting asynchronous");
    (1 to 10).map(i => incrementor !! i).foreach(future => printf("returned=%d\n", future()))
    println("Done");
  }
}

