import scala.collection.JavaConversions._
import org.ektorp.CouchDbConnector
import org.ektorp.impl.StdCouchDbInstance
import org.ektorp.http.StdHttpClient
import org.ektorp.http.StdHttpClient.Builder
import org.apache.hadoop.conf._
import org.codehaus.jackson.annotate.JsonProperty
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.JsonNode
import scala.actors.{Actor,Exit}
import scala.actors.Actor._

object tweetsToCouch {
  def main(args: Array[String]) {
    val Array(file, dbName) = args.length match {
      case 2 => args
      case _ => println("\nUsage: <file> <dbName>\n"); sys.exit(0)
    }
    printf("file: %s   dbName: %s\n", file, dbName)

    val before = System.currentTimeMillis
    val jsonReader = actor {
      var subscribers = 0
      val tweets = scala.io.Source.fromFile(file).getLines.map(parseJson).filter(_ != null)
      loop {
        react {
          case "subscribe" => println("got a subscriber " + subscribers); subscribers += 1; 
          case "unsubscribe" => println("lost a subscriber " + subscribers); subscribers -= 1;  if (subscribers == 0) { println("Time: " + (System.currentTimeMillis - before) + " s"); exit} 
          case "next" =>  reply(if (tweets.hasNext) Some(tweets.next) else None)
          case x: Any => println("Json reader doesn't know what to do with " + x)
        }
      }
    }

    val couch = getCouchClient("192.168.254.1", 5984)
    val db = couch.createConnector(dbName, true) // Just to ensure that the table exists before the writters begin.
    for (i <- 1 to 3) {
      (new TweetWriter(dbName, jsonReader)).start
    }

    class TweetWriter(dbName: String, jsonReader: Actor) extends Actor {
      def act {
        try {
          val couch = getCouchClient("192.168.254.1", 5984)
          val db = couch.createConnector(dbName, true)

          jsonReader ! "subscribe"
          var tweetsWritten = 0
          var duplicates = 0
          var conflicts = 0
          try {
            var done = false
            while (!done) {
              jsonReader !? "next" match {
                case None => done = true; println("done")
                case Some(tweet: JsonNode) => 
                  val id = tweet.path("id_str").toString.replace("\"", "")
                  var containsId = db.contains(id)
                  try { 
                    if (containsId) {
                      duplicates += 1
                    }
                    else {
                      db.create(id, tweet)
                      tweetsWritten += 1 
                    }
                  } catch {
                    case ex: org.ektorp.UpdateConflictException => conflicts += 1; println("It looks like there's conflict with a tweet ID: " + id)
                  }
                case x: Any => "I got " + x + " when I was expeting Some(tweet)"
              }
            }
            printf("Wrote %d tweets. Skipped duplicate %d tweets. Had %d conflicts.", tweetsWritten, duplicates, conflicts)
          }
          finally {
            jsonReader ! "unsubscribe"
          }
        } catch {
          case ex: Exception => println("Caught exception in TweetWriter: " + ex)
        }
      }
    }
  }


  def oneActor(args: Array[String]) {
    val Array(file, dbName) = args.length match {
      case 2 => args
      case _ => println("\nUsage: <file> <dbName>\n"); sys.exit(0)
    }
    printf("file: %s   dbName: %s\n", file, dbName)

    val couch = getCouchClient("192.168.254.1", 5984)
    val db = couch.createConnector(dbName, true)

    val jsonReader = actor {
      var subscribers = 0
      val tweets = scala.io.Source.fromFile(file).getLines.map(parseJsonToMap).filter(_ != null)
      loop {
        react {
          case "subscribe" => println("got a subscriber " + subscribers); subscribers += 1; 
          case "unsubscribe" => println("lost a subscriber " + subscribers); subscribers -= 1; if (subscribers == 0) exit
          case "next" => println("Asked for next"); reply(if (tweets.hasNext) Some(tweets.next) else None)
          case x: Any => println("Json reader doesn't know what to do with " + x)
        }
      }
    }

    jsonReader ! "subscribe"
    var done = false
    var tweetsWritten = 0
    while (!done) {
      jsonReader !? "next" match {
        case None => done = true; println("done")
        case Some(tweet) => println("Attempting to write tweet " + tweetsWritten); db.create(tweet); tweetsWritten += 1
        case x: Any => "I got " + x + " when I was expeting Some(tweet)"
      }
    }
    println("out of loop")
    jsonReader ! "unsubscribe"
    println("Wrote " + tweetsWritten + " tweets")
  }


  def theEasyWay(args: Array[String]) {
    val Array(file, dbName) = args.length match {
      case 2 => args
      case _ => println("\nUsage: <file> <dbName>\n"); sys.exit(0)
    }
    printf("file: %s   dbName: %s\n", file, dbName)

    val couch = getCouchClient("192.168.254.1", 5984)
    val db = couch.createConnector(dbName, true)

    val before = System.currentTimeMillis
    val tweets = scala.io.Source.fromFile(file).getLines.map(parseJsonToMap).filter(_ != null)
    tweets.foreach(db.create(_))
    println("Time: " + (System.currentTimeMillis - before) + " s")
  }

  def getType(a: Any) = a match {
    case ref: AnyRef => ref.getClass
    case atomic: Any => "atomic: " + atomic
  }

  val jsonDeserializer = new ObjectMapper
  def parseJson(json: String): JsonNode = {
    try {
      jsonDeserializer.readTree(json)
    } catch {
      case ex: Exception => println("Exception: " + ex); null
    }
  }
  def parseJsonToMap(json: String): java.util.Map[String, JsonNode] = {
    try {
      jsonDeserializer.readValue(json, classOf[java.util.Map[String, JsonNode]])
    } catch {
      case ex: Exception => println("Exception: " + ex); null
    }
  }




  def createWriteAndReadJunk {
    val dbName = "junk"
    val couch = getCouchClient("192.168.254.1", 5984)
    println("HuzzaH?")
    try {
      couch.deleteDatabase(dbName)
    } catch {
      case notFound: org.ektorp.DocumentNotFoundException => println(dbName + " didn't exist.  You won't miss it.")
    }
    couch.createDatabase(dbName)
    printf("Databases:\n%s\n", couch.getAllDatabases.map("  " + _).mkString("\n"))
    val db = couch.createConnector(dbName, true)

    val id = "a"
    val d = new Dummy
    d.setName(id)
    d.setId(id)
    println("About to create: " + d)
    db.create(d)
    println("Reading back: ")
    println("  " + db.get(classOf[Dummy], id))
  }

  def getCouchClient(host: String, port: Int) = new StdCouchDbInstance(new StdHttpClient.Builder().host(host).port(port).build)
}

class Dummy() {
  override def toString = "id: '%s'  revision: '%s'  name: '%s'".format(getId, getRevision, getName)

  private var name: String = _
  def getName = this.name
  def setName(name: String) = this.name = name

  @JsonProperty("_id")
  private var id: String = _
  def getId: String = this.id
  def setId(id: String) = this.id = id

  @JsonProperty("_rev")
  private var revision: String = _
  def getRevision: String = this.revision
  def setRevision(revision: String) = this.revision = revision
}
