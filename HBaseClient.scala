import java.util.Date
import scala.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue

import org.apache.hadoop.conf._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

import com.twitter.json.Json

object HBaseClient {
  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def doubleToByteArray(d: Double): Array[Byte] = Bytes.toBytes(d)
  implicit def bigDecimalToByteArray(b: BigDecimal): Array[Byte] = Bytes.toBytes(b.toDouble)
  type JsonObject = Map[Any,Any]
  val hbaseTimer = new Timer
  val jsonTimer = new Timer

  def printExists(path : String) {
    println("path " + path + " exists " + new java.io.File(path).exists.toString);
  }

  def main(args: Array[String]) {
    val config = HBaseConfiguration.create()
    //List(
    //  "/hadoop/conf/core-site.xml",
    //  "/hadoop/conf/hdfs-site.xml",
    //  "/hadoop/conf/mapred-site.xml",
    //  "/hbase/conf/hbase-site.xml"
    //)foreach config.addResource

    //configFiles foreach printExists
    //configFiles (configFile => config.addResource(configFile))
    println("hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"))
    config.set("hbase.zookeeper.quorum", "system2,system3,system4")
    println("hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"))
    val admin = new HBaseAdmin(config)
    val tableName = "tweets"
    val columnFamilyName = "raw"

    if (!doesTableExist(admin, tableName)) createTable(admin, tableName, columnFamilyName)

    val table = new HTable(config, tableName)
    timeIt(() => {
      println("Reading from: " + args(0))
      Source.fromFile(args(0)).getLines.foreach(line => createRowFor(line, table, columnFamilyName))
    })
    println("Total HBase Put time: " + (hbaseTimer.getTotal / 1000.0) + " s")
    println("Total JSON parse time: " + (jsonTimer.getTotal / 1000.0) + " s")
  }

  def timeIt[T](callback: () => T) {
    val startTime = System.currentTimeMillis;
    val ret = callback()
    println("Time: " + ((System.currentTimeMillis - startTime) * 1E-3 + " s")) 
    ret
  }

  def createRowFor(line: String, table: HTable, columnFamilyName : String) {
    jsonTimer.go
    try {
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
    } catch { 
      case e: Exception => println("Got exception when trying to put " + line + "\n  Error: " + e);
    }
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
