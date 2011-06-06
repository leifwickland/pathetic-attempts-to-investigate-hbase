import java.util.Date
import scala.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._

import java.util.regex._
import java.util.Date
import java.text.SimpleDateFormat

object logToHbase {
  val startTime = System.currentTimeMillis
  def main(args: Array[String]) {
    val table = startHbase(args(0), Seq("common", "hits"))
    val before = System.currentTimeMillis
    parseLog(args(1)).collect{ case Some(matcher) => LogLine.fromMatch(matcher) }.filter{_ != null}.foreach(writeLogToHBase(table, _))
    val elapsed = System.currentTimeMillis - before
    table.close
    println("Time to write %d records: %d ms.  %d/sec".format(recordsWrittenToHBase, elapsed, (recordsWrittenToHBase / (elapsed / 1000.0)).toInt))
  }

  object LogLine {
    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:kk:mm:ss Z")
    def fromMatch(matcher: Matcher): LogLine = { 
      try {
        return new LogLine(matcher.group(1), dateFormat.parse(matcher.group(2)), matcher.group(3), matcher.group(4), matcher.group(5).toInt,
        matcher.group(6), matcher.group(7))
      } catch {
        case e: Exception => return null
      }
    }
  }
  case class LogLine(ipAddress: String, timestamp: Date, method: String, url: String, status: Int, referrer: String, userAgent: String) {
    def id = MD5.hash(ipAddress + "-" + userAgent)
  }


  var recordsWrittenToHBase = 0

  def writeLogToHBase(table: HTable, logLine: LogLine) {
    val put = new Put(logLine.id)
    put.add("common", "IP", logLine.ipAddress)
    put.add("common", "userAgent", logLine.userAgent)
    val hitKey = logLine.timestamp.getTime.toString + "-" + logLine.url
    val hitValue = """{"url":"%s", "status": %d, "referrer":"%s", "timestamp": %d, "method": "%s"}""".format(logLine.url, logLine.status,
      logLine.referrer, logLine.timestamp.getTime, logLine.method)
    put.add("hits", hitKey, hitValue)
    table.put(put)
    recordsWrittenToHBase += 1
    if (recordsWrittenToHBase % 1000 == 0) {
      println("%07.2f %d: %s".format(0.001 * (System.currentTimeMillis - startTime), recordsWrittenToHBase, logLine))
    }
  }

  def parseLog(filename: String) = {
    scala.io.Source.fromFile(filename).getLines.map(parseLogLine(_))

    //   1: 172.22.4.194
    //   2: 08/Apr/2010:13:39:27 -0600
    //   3: GET
    //   4: /app/chat/chat_launch/session/L3RpbWUvMTI3MDc1NTU2My9zaWQvb0xQWE5WWWo%3D
    //   5: 200
    //   6: http://dhaynes105fixes.teton.rightnowtech.com/
    //   7: Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 (.NET CLR 3.5.30729)
  }

  val regex = Pattern.compile("""^(\S+) - - \[([^\[]+)\] "([^"]+) ([^"]+) [^"]+" (\S+) \S+ "([^"]+)" "([^"]+)"\s*$""")
  def parseLogLine(line: String) = {
    val matcher = regex.matcher(line)
    matcher.matches match {
      case false => None
      case true => Some(matcher)
    }
  }

  def startHbase(tableName: String, columnFamilyNames: Seq[String]) = {
    val config = HBaseConfiguration.create()
    val admin = new HBaseAdmin(config)

    if (!doesTableExist(admin, tableName)) createTable(admin, tableName, columnFamilyNames)
    val table = new HTable(config, tableName)
    //table.setAutoFlush(false)
    table
  }


  def doesTableExist(admin: HBaseAdmin, tableName: String): Boolean = {
    admin.listTables().exists(table => Bytes.equals(table.getName, tableName))
  }

  def createTable(admin: HBaseAdmin, tableName: String, columnFamilyNames: Seq[String]) {
    val table = new HTableDescriptor(tableName);
    columnFamilyNames.foreach(table addFamily new HColumnDescriptor(_))
    admin createTable table
    println("Created " + tableName + " with " + columnFamilyNames.mkString(", "));
  }

  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def doubleToByteArray(d: Double): Array[Byte] = Bytes.toBytes(d)
  implicit def bigDecimalToByteArray(b: BigDecimal): Array[Byte] = Bytes.toBytes(b.toDouble)
  implicit def IntegerToByteArray(i: java.lang.Integer): Array[Byte] = Bytes.toBytes(i)
}

object MD5 {
  import java.security._
  import java.math._
  def hash(s: String) = {
    val m = MessageDigest.getInstance("MD5")
    val b = Bytes.toBytes(s)
    m.update(b, 0, b.length)
    new BigInteger(1, m.digest()).toString(16)
  }
}
