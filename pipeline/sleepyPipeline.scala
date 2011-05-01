import scala.actors.Actor
import scala.actors.Actor._
import java.lang.Thread
import java.util.Random

object sleepyPipeline {
  def main(args : Array[String]) {
    println("Main starting")
    val left = new Left
    left.start
    for (i <- 1 to 10) {
      var middle = new Middle(left, i.toString)
      middle.start
      for (j <- 1 to 10) { 
        val right = new Right(middle, (j + (i-1) * 4).toString)
        right.start
      }
    }
    println("Main exiting")
  }
}

class Left extends Actor {
  var i = 0
  var listenerCount = 0

  def join {
    listenerCount += 1
    printf("Somebody (now %d) loves me!\n", listenerCount)
  }

  def leave {
    listenerCount -= 1
    printf("Somebody left! (now %d)\n", listenerCount)
    if (listenerCount <= 0) {
      println("No more listeners")
      exit("no more litsteners")
    }
  }

  def act {
    println("Top Left act")
    loop {
      react {
        case Next => i += 1; reply(i)
        case Join => join
        case Leave => leave
      }
    }
  }
}

class Middle(left: Left, name: String) extends Actor {
  var listenerCount = 0
  override def act {
    println("Top Middle act")
    val rand = new Random
    left ! Join
    loop {
      react {
        case Next => Thread.sleep(rand.nextInt(100)); reply(left !? Next)
        case Join => {
          listenerCount += 1
          printf("Middle %s got a new listener and has %d listeners\n", name, listenerCount)
        }
        case Leave => {
          listenerCount += -1
          printf("Middle %s lost a listener and has %d listeners\n", name, listenerCount)
          if (listenerCount <= 0) {
            left ! Leave
            exit("no more litsteners")
          }
        }
      }
    }
  }
}

class Right(middle: Middle, name: String) extends Actor {
  override def act {
    println("Top Right act")
    printf("Right %s starting up!", name);
    middle ! Join
    for (i <- 1 to 100) { 
      printf("Right %s at %d got %d\n", name, i, middle !? Next)
    }
    middle ! Leave
    printf("Right %s is spent!", name);
  }
}
case class Next()
case class Join()
case class Leave()

