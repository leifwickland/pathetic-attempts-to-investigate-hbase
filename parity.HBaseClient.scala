import java.util.Date
import scala.io._
import scala.collection.JavaConversions._

import org.apache.hadoop.conf._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

object HBaseClient {
  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def byteArrayToString(b: Array[Byte]): String = Bytes.toString(b)

  def main(args: Array[String]) {
    val config = HBaseConfiguration.create
    val admin = new HBaseAdmin(config)
    val tableName = "tweets"

    if (!doesTableExist(admin, tableName)) createTable(admin, tableName, columnFamilyName)

    val table = new HTable(config, tableName)
    val key1 = "id1"
    val columnFamilyName = "raw"
    
    var p1 = new Put(key1)
    val columns = Map(
      "a" -> "Leif Was Here", 
      "b" -> new Date().toString()
    )
    columns foreach( (column) => p1.add(columnFamilyName, column._1, column._2) )
    table put p1

    val result = table.get(new Get(key1))
    println("Got: " + result)

    println("\nBeginning Scan");
    for (scannerResult <- table.getScanner(new Scan())) {
      println("Scan Result: " + scannerResult);
      for (column <- columns) {
        println("  Trying to get " + columnFamilyName + ":" + column._1)
        for (keyValue <- scannerResult.getColumn(columnFamilyName, column._1)) {
          println("    Got value: " + Bytes.toString(keyValue.getFamily) + ":" + Bytes.toString(keyValue.getQualifier) + "(" +
            keyValue.getTimestamp + "):" + Bytes.toString(keyValue.getValue()))
        }
      }
    }
  }

  def doesTableExist(admin: HBaseAdmin, tableName: String): Boolean = {
    admin.listTables().exists((table) => Bytes.equals(table.getName, tableName))
  }

  def createTable(admin: HBaseAdmin, tableName: String, columnFamilyName: String) {
    val table = new HTableDescriptor(tableName);
    table addFamily new HColumnDescriptor(columnFamilyName)
    admin createTable table
    println("Created " + tableName + " with " + columnFamilyName);
  }
}
