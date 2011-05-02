import scala.actors.Actor
import scala.actors.Actor._
import java.lang.Thread
import java.util.Random

object sleepyPipeline {
  def main(args : Array[String]) {
    if (args.length != 3) {
      println("Usage: middles rights sleepMillis")
      return
    }
    val middleCount = args(0).toInt
    val rightCount = args(1).toInt
    val sleepMillis = args(2).toInt
    println("Main starting: " + middleCount + " " + rightCount + " " + sleepMillis)
    val left = new Left
    left.start
    
    val rights = new scala.collection.mutable.ListBuffer[Right]
    for (i <- 1 to middleCount) {
      var middle = new Middle(left, i.toString)
      middle.start
      for (j <- 1 to rightCount) { 
        val right = new Right(middle, (j + (i-1) * rightCount).toString, sleepMillis)
        right.start
        //rights.append(right)
      }
    }

    //rights.foreach(right => right.start)
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
      println("No more listeners. Exiting with i = " + i)
      exit("no more litsteners")
    }
  }

  def act {
    loop {
      receive {
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
    left ! Join
    loop {
      receive {
        case Next => reply(left !? Next)
        case Join => {
          listenerCount += 1
          //printf("Middle %s got a new listener and has %d listeners\n", name, listenerCount)
        }
        case Leave => {
          listenerCount -= 1
          //printf("Middle %s lost a listener and has %d listeners\n", name, listenerCount)
          if (listenerCount <= 0) {
            left ! Leave
            exit("no more litsteners")
          }
        }
      }
    }
  }
}

class Right(middle: Middle, name: String, sleepMillis: java.lang.Integer) extends Actor {
  val rand = new Random
  override def act {
    middle ! Join
    printf("Right %s is starting\n", name);
    var i = 0
    this ! Next
    loop {
      react {
        case Next => {
          i += 1
          if (sleepMillis > 0) Thread.sleep(rand.nextInt(sleepMillis));
          var received = middle !? Next
          printf("Right %s at %d got %d\n", name, i, received)
          if (i < 100) {
            this ! Next
          }
          else {
            middle ! Leave
            printf("Right %s is spent, but last got %d!\n", name, received);
            exit
          }
        }
      }
    }
    /*
    for (i <- 1 to 100) { 
      printf("Right %s at %d got %d\n", name, i, middle !? Next)
    }
    middle ! Leave
    printf("Right %s is spent!", name);
    */
  }
}

case class Next()
case class Join()
case class Leave()

