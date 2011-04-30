import java.util.{Date,Random}
import scala.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue

import org.apache.hadoop.conf._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

import com.twitter.json.Json

object HBaseRand {
  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def DoubleToByteArray(d: Double): Array[Byte] = Bytes.toBytes(d)
  implicit def BigDecimalToByteArray(b: BigDecimal): Array[Byte] = Bytes.toBytes(b.toDouble)
  type JsonObject = Map[Any,Any]
  val hbaseTimer = new Timer
  val jsonTimer = new Timer
  val totalTimer = new Timer

  def printExists(path : String) {
    println("path " + path + " exists " + new java.io.File(path).exists.toString);
  }

  def main(args: Array[String]) {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "system2,system3,system4")
    val admin = new HBaseAdmin(config)
    val tableName = "randomdata"
    val columnFamilyName = "raw"

    if (!doesTableExist(admin, tableName)) createTable(admin, tableName, columnFamilyName)
    
    val numberOfPuts = 0.5e6.toInt
    val threadCount = 16
    val threads = (1 to threadCount) map(i => {
      new Thread {
        override def run() {
          println("Starting thread " + i)
          val table = new HTable(config, tableName)

          val random = new Random
          timeIt(() => {
            for (j <- 0 until numberOfPuts) {
              val put = new Put((i * numberOfPuts + j).toString)

              val columns = Map(
                "a" -> (random.nextLong.toString() + random.nextLong.toString() + random.nextInt.toString())
                //"b" -> random.nextLong.toString,
                //"c" -> random.nextLong.toString,
                //"d" -> random.nextLong.toString,
                //"e" -> random.nextLong.toString
              )

              for (column <- columns) {
                put.add(columnFamilyName, column._1, column._2)
              }
              hbaseTimer.go
              table.put(put)
              hbaseTimer.stop
            }
          })
          println("Total HBase time " + (hbaseTimer.getTotal / 1000.0) + " s for " + numberOfPuts + " puts")
        }
      }
    })
    totalTimer.go
    threads.foreach(t => t.start())
    threads.foreach(t => t.join()) 
    totalTimer.stop
    println("Total time for " + (numberOfPuts * threadCount) + " puts on " + threadCount + " threads: " + (hbaseTimer.getTotal / 1000.0) + " s")
  }

  def timeIt[T](callback: () => T) {
    val startTime = System.currentTimeMillis;
    val ret = callback()
    println("Time: " + ((System.currentTimeMillis - startTime) * 1E-3 + " s")) 
    ret
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
