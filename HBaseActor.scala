import java.util.Date
import scala.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue
import scala.actors.{Actor,Exit}
import scala.actors.Actor._

import org.apache.hadoop.conf._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

import com.twitter.json.Json

object HBaseActor {
  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def DoubleToByteArray(d: Double): Array[Byte] = Bytes.toBytes(d)
  implicit def BigDecimalToByteArray(b: BigDecimal): Array[Byte] = Bytes.toBytes(b.toDouble)
  type JsonObject = Map[Any,Any]
  val hbaseTimer = new Timer
  val jsonTimer = new Timer
  val totalTimer = new Timer

  def printExists(path: String) {
    println("path " + path + " exists " + new java.io.File(path).exists.toString);
  }

  def log(message: String) {
    //printf("(%9.3f) %s\n", System.currentTimeMillis() / 1000.0, message)
  }

  def main(args: Array[String]) {
    val timerPrinter = actor {
      loop {
        react {
          case Exit => {
            totalTimer.stop
            println("Total Time: " + (totalTimer.getTotal / 1000.0) + " s")
            println("Total HBase Put time: " + (hbaseTimer.getTotal / 1000.0) + " s")
            println("Total JSON parse time: " + (jsonTimer.getTotal / 1000.0) + " s")
            exit()
          }
          case x : Any => log("timerPrinter got something unexpected: " + x); exit()
        }
      }
    }

    val hbaseWriter = actor {
      val config = HBaseConfiguration.create()
      println("hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"))
      config.set("hbase.zookeeper.quorum", "system2,system3,system4")
      println("hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"))
      val admin = new HBaseAdmin(config)
      val tableName = "tweets"
      val columnFamilyName = "raw"
      if (!doesTableExist(admin, tableName)) createTable(admin, tableName, columnFamilyName)
      val table = new HTable(config, tableName)

      loop {
        react {
          case tweet : JsonObject => log("hbaseWriter got tweet"); hbaseTimer.go; writeTweet(tweet); hbaseTimer.stop
          case Exit => log("hbaseWriter got Exit"); timerPrinter ! Exit; exit()
          case x : Any => log("hbaseWriter got something unexpected: " + x); exit()
        }

        def writeTweet(tweet : JsonObject) {
          val put = new Put(getStringFromJson(tweet, Queue("id_str")))

          val columns = Map(
            "user_id" -> Queue("user", "id_str"),
            "created_at" -> Queue("created_at"),
            "text" -> Queue("text")
          )
          for (column <- columns) {
            put.add(columnFamilyName, column._1, getStringFromJson(tweet, column._2))
          }
          val coordinates = extractCoordinatesFrom(tweet)
          put.add(columnFamilyName, "longitude", coordinates(0))
          put.add(columnFamilyName, "latitude", coordinates(1))
          hbaseTimer.go
          table.put(put)
          hbaseTimer.stop
        }
      }
    }

    val jsonParser = actor {
      loop {
        react {
          case line : String => log("jsonParser got line"); jsonTimer.go; val tweet = Json.parse(line.trim); jsonTimer.stop; hbaseWriter ! tweet
          case Exit => log("jsonParser got Exit"); hbaseWriter ! Exit; exit()
          case x : Any => log("jsonParser got something unexpected: " + x); exit()
        }
      }
    }


    totalTimer.go
    Source.fromFile(args(0)).getLines.foreach(line => jsonParser ! line)
    println("Done reading files")
    jsonParser ! Exit
    println("Sent Json Exit")
  }

  def timeIt[T](callback: () => T) {
    val startTime = System.currentTimeMillis;
    val ret = callback()
    println("Time: " + ((System.currentTimeMillis - startTime) * 1E-3 + " s")) 
    ret
  }

  def createRowFor(line: String, table: HTable, columnFamilyName : String) {
    jsonTimer.go
    val tweet = Json.parse(line.trim).asInstanceOf[JsonObject]
    jsonTimer.stop
    val put = new Put(getStringFromJson(tweet, Queue("id_str")))

    val columns = Map(
      "user_id" -> Queue("user", "id_str"),
      "created_at" -> Queue("created_at"),
      "text" -> Queue("text")
    )
    for (column <- columns) {
      put.add(columnFamilyName, column._1, getStringFromJson(tweet, column._2))
    }
    val coordinates = extractCoordinatesFrom(tweet)
    put.add(columnFamilyName, "longitude", coordinates(0))
    put.add(columnFamilyName, "latitude", coordinates(1))
    hbaseTimer.go
    table.put(put)
    hbaseTimer.stop
  }

  def extractCoordinatesFrom(tweet : JsonObject) : List[BigDecimal] = {
    val coordinates = getFromJson(tweet, Queue("coordinates"))
    var decimalCoordinates : List[BigDecimal] = null
    if (coordinates != null) {
      return getFromJson(coordinates.asInstanceOf[JsonObject], Queue("coordinates")).asInstanceOf[List[BigDecimal]]
    }
    var boundingBox = getFromJson(tweet, Queue("place", "bounding_box", "coordinates")).asInstanceOf[List[List[List[BigDecimal]]]](0)
    return List(
      (boundingBox(0)(0) + boundingBox(1)(0) + boundingBox(2)(0) + boundingBox(3)(0)) / 4.0,
      (boundingBox(0)(1) + boundingBox(1)(1) + boundingBox(2)(1) + boundingBox(3)(1)) / 4.0
    )
  }

  def doesTableExist(admin: HBaseAdmin, tableName: String): Boolean = {
    admin.listTables().exists(table => Bytes.equals(table.getName, tableName))
  }

  def createTable(admin: HBaseAdmin, tableName: String, columnFamilyName: String) {
    val table = new HTableDescriptor(tableName);
    table addFamily new HColumnDescriptor(columnFamilyName)
    admin createTable table
    println("Created " + tableName + " with " + columnFamilyName);
  }

  def getStringFromJson(json: JsonObject, keyPath: Queue[String]): String = {
    getFromJson(json, keyPath).asInstanceOf[String]
  }

  def getFromJson(json: JsonObject, keyPath: Queue[String]): Any = {
    val key = keyPath.dequeue
    try {
      val value = json(key)
      if (keyPath.isEmpty) return value
      return getFromJson(value.asInstanceOf[JsonObject], keyPath)
    } catch {
      case e: Exception => println("Got exception when trying to read " + key + " from " + json); return null;
    }
  }
}

object Done {
}

class Timer {
  private var start:Long = 0L
  var total:Long = 0L
  def go {
    start = System.currentTimeMillis
  }
  def stop {
    total += System.currentTimeMillis - start
  }
  def getTotal : Long = {
    total
  }
}
