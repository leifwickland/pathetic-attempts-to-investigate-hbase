import scala.collection.JavaConversions._
import org.ektorp.CouchDbConnector
import org.ektorp.impl.StdCouchDbInstance
import org.ektorp.http.StdHttpClient
import org.ektorp.http.StdHttpClient.Builder
import org.apache.hadoop.conf._
import org.codehaus.jackson.annotate.JsonProperty
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node._
import scala.actors.{Actor,Exit}
import scala.actors.Actor._
import java.util.concurrent.Future
import org.apache.http.entity._
import org.apache.http.HttpResponse
import org.apache.http.client.methods._
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient
import org.apache.http.nio.client.HttpAsyncClient

object tweetsToCouch {
  def main(args: Array[String]) {
    //multithreaded(args)
    writeUsersWithNestedTweets(args)
  }

  def writeUsersWithNestedTweets(args: Array[String]) {
    val Seq(file: String, dbName: String, server: String, threadCount: Int, useId: Boolean) = args.length match {
      case 5 => Seq(args(0), args(1), args(2), args(3).toInt, args(4).toBoolean)
      case _ => println("\nUsage: <file> <dbName> <server> <threadCount> <useId>\n"); sys.exit(0)
    }
    val before = System.currentTimeMillis
    val jsonReader = actor {
      var subscribers = 0
      var totalTweetsWritten = 0
      var totalDuplicates = 0
      var totalConflicts = 0
      val tweets = scala.io.Source.fromFile(file).getLines.filter(_.trim.length > 0).map(parseJson).filter(_ != null).zipWithIndex
      loop {
        react {
          case "subscribe" => println("got a subscriber " + subscribers); subscribers += 1; 
          case ("unsubscribe", tweetsWritten: Int, duplicates: Int, conflicts: Int) =>
            println("lost a subscriber " + subscribers); 
            subscribers -= 1; 
            totalTweetsWritten += tweetsWritten
            totalDuplicates += duplicates
            totalConflicts += conflicts
            if (subscribers == 0) {
              println("Time: " + 0.001 * (System.currentTimeMillis - before) + " s");
              printf("Total: Wrote %d tweets. Skipped duplicate %d tweets. Had %d conflicts.\n", totalTweetsWritten, totalDuplicates, totalConflicts)
              exit
            }
          case "next" =>  
            if (tweets.hasNext) {
              val (next, i) = tweets.next
              if (i % 1000 == 0) println("Read the " + i + "th tweet from " + file + "  Time since start=" + 0.001 * (System.currentTimeMillis - before))
              reply(Some(next))
            }
            else {
              reply(None)
            }
          case x: Any => println("Json reader doesn't know what to do with " + x)
        }
      }
    }

    val db = getCouchClient(server, 5984).createConnector(dbName, true) // Just to ensure that the table exists before the writters begin.

    for (i <- 1 to threadCount) {
      (new TweetWriter(server, dbName, jsonReader, useId)).start
    }

    class TweetWriter(server: String, dbName: String, jsonReader: Actor, useId: Boolean) extends Actor {
      def act {
        try {
          val couch = getCouchClient(server, 5984)
          val db = couch.createConnector(dbName, true)

          jsonReader ! "subscribe"
          var tweetsWritten = 0
          var duplicates = 0
          var conflicts = 0
          try {
            var done = false
            while (!done) {
              jsonReader !? "next" match {
                case None => done = true;
                case Some(tweet: JsonNode) => 
                  writeTweetIntoUser(tweet, db) match {
                    case None => duplicates += 1
                    case Some(c) => 
                       tweetsWritten +=1
                       conflicts += c
                  }
                case x: Any => "I got " + x + " when I was expecting Some(tweet)"
              }
            }
            printf("Wrote %d tweets. Skipped duplicate %d tweets. Had %d conflicts.\n", tweetsWritten, duplicates, conflicts)
          }
          finally {
            jsonReader ! ("unsubscribe", tweetsWritten, duplicates, conflicts)
          }
        } catch {
          case ex: Exception => println("Caught exception in TweetWriter: " + ex); ex.printStackTrace
        }
      }
      val jsonSerializer = new ObjectMapper
      def writeTweetIntoUser(tweet: JsonNode, db: CouchDbConnector): Option[Int] = {
        val id = tweet.path("id_str").toString.replace("\"", "")
        val userId = tweet.path("user").asInstanceOf[JsonNode].path("id").toString
        val userName = tweet.path("user").asInstanceOf[JsonNode].path("screen_name").toString
        val initialUser = """{"user":%s, "tweets":{}}""".format(userName)
        var user = jsonSerializer.readTree(initialUser)
        user.path("tweets").asInstanceOf[ObjectNode].put(id, tweet)

        try {
          db.create(userId, user)
          return Some(0)
        } catch {
          case ex: org.ektorp.UpdateConflictException => //println("It looks like user ID " + userId + " already exists\n  " + ex)
        }
        0.to(5).foreach{retry => 
          try {
            user = db.get(classOf[JsonNode], userId).asInstanceOf[ObjectNode]
            user.path("tweets").asInstanceOf[ObjectNode].put(id, tweet)
            db.update(user)
            return Some(retry)
          } catch {
            case ex: org.ektorp.UpdateConflictException => println("On " + retry + " got conflict updating " + userId + "\n  " + ex)
          }
        }
        return None
      }
    }
  }

  def withAsyncIo(args: Array[String]) {
    val Seq(file: String, dbName: String, server: String, threadCount: Int, useId: Boolean) = args.length match {
      case 5 => Seq(args(0), args(1), args(2), args(3).toInt, args(4).toBoolean)
      case _ => println("\nUsage: <file> <dbName> <server> <threadCount> <useId>\n"); sys.exit(0)
    }
    val before = System.currentTimeMillis
    var httpClient = new DefaultHttpAsyncClient
    try {
      httpClient.start
      val postUrl = "http://%s:5984/%s/".format(server, dbName)
      val requests = scala.io.Source.fromFile(file).getLines.map(_.trim).filter(_.length > 0).map { tweet =>
        httpClient.execute(newPost(postUrl, tweet), null)
      }
      val requestCount = requests.length
      var requestsDone = requests.count(_.isDone)
      while (requestCount > requestsDone) {
        println(requestsDone + " of " + requestCount + " are done")
        Thread.sleep(100)
        requestsDone = requests.count(_.isDone)
      }

    } finally {
      httpClient.shutdown
    }
    def newPost(url: String, body: String) = {
      var post = new HttpPost(url)
      post.setEntity(new StringEntity(body))
      post.addHeader("Content-Type", "application/json")
      post
    }
  }

  def multithreaded(args: Array[String]) {
    val Seq(file: String, dbName: String, server: String, threadCount: Int, useId: Boolean) = args.length match {
      case 5 => Seq(args(0), args(1), args(2), args(3).toInt, args(4).toBoolean)
      case _ => println("\nUsage: <file> <dbName> <server> <threadCount> <useId>\n"); sys.exit(0)
    }
    printf("file: %s   dbName: %s\n", file, dbName, server)

    val before = System.currentTimeMillis
    val jsonReader = actor {
      var subscribers = 0
      var totalTweetsWritten = 0
      var totalDuplicates = 0
      var totalConflicts = 0
      val tweets = scala.io.Source.fromFile(file).getLines.filter(_.trim.length > 0).map(parseJson).filter(_ != null).zipWithIndex
      loop {
        react {
          case "subscribe" => println("got a subscriber " + subscribers); subscribers += 1; 
          case ("unsubscribe", tweetsWritten: Int, duplicates: Int, conflicts: Int) =>
            println("lost a subscriber " + subscribers); 
            subscribers -= 1; 
            totalTweetsWritten += tweetsWritten
            totalDuplicates += duplicates
            totalConflicts += conflicts
            if (subscribers == 0) {
              println("Time: " + 0.001 * (System.currentTimeMillis - before) + " s");
              printf("Total: Wrote %d tweets. Skipped duplicate %d tweets. Had %d conflicts.\n", totalTweetsWritten, totalDuplicates, totalConflicts)
              exit
            }
          case "next" =>  
            if (tweets.hasNext) {
              val (next, i) = tweets.next
              if (i % 1000 == 0) println("Read the " + i + "th tweet from " + file + "  Time since start=" + 0.001 * (System.currentTimeMillis - before))
              reply(Some(next))
            }
            else {
              reply(None)
            }
          case x: Any => println("Json reader doesn't know what to do with " + x)
        }
      }
    }

    val db = getCouchClient(server, 5984).createConnector(dbName, true) // Just to ensure that the table exists before the writters begin.

    for (i <- 1 to threadCount) {
      (new TweetWriter(server, dbName, jsonReader, useId)).start
    }

    class TweetWriter(server: String, dbName: String, jsonReader: Actor, useId: Boolean) extends Actor {
      def act {
        try {
          val couch = getCouchClient(server, 5984)
          val db = couch.createConnector(dbName, true)

          jsonReader ! "subscribe"
          var tweetsWritten = 0
          var duplicates = 0
          var conflicts = 0
          try {
            var done = false
            while (!done) {
              jsonReader !? "next" match {
                case None => done = true;
                case Some(tweet: JsonNode) => 
                  val id = tweet.path("id_str").toString.replace("\"", "")
                  try { 
                    if (useId) {
                      if (db.contains(id)) {
                        duplicates += 1
                      }
                      else {
                        db.create(id, tweet)
                        tweetsWritten += 1 
                      }
                    }
                    else {
                      db.create(tweet)
                      tweetsWritten += 1 
                    }
                  } catch {
                    case ex: org.ektorp.UpdateConflictException => conflicts += 1; println("It looks like there's conflict with a tweet ID: " + id + "\n  " + ex)
                  }
                case x: Any => "I got " + x + " when I was expecting Some(tweet)"
              }
            }
            printf("Wrote %d tweets. Skipped duplicate %d tweets. Had %d conflicts.\n", tweetsWritten, duplicates, conflicts)
          }
          finally {
            jsonReader ! ("unsubscribe", tweetsWritten, duplicates, conflicts)
          }
        } catch {
          case ex: Exception => println("Caught exception in TweetWriter: " + ex); ex.printStackTrace
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
        case x: Any => "I got " + x + " when I was expecting Some(tweet)"
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
      case ex: Exception => println("Exception deserializing tweet JSON: " + ex + "\n  " + json); null
    }
  }
  def parseJsonToMap(json: String): java.util.Map[String, JsonNode] = {
    try {
      jsonDeserializer.readValue(json, classOf[java.util.Map[String, JsonNode]])
    } catch {
      case ex: Exception => println("Exception deserializing tweet JSON: " + ex + "\n  " + json); null
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
