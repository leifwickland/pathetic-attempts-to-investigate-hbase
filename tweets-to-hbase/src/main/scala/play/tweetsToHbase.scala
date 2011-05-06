package play

// The following was largely stolen from https://github.com/cageface/scala-hadoop-example/blob/master/WordCount.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text,IntWritable,Writable,NullWritable}
import org.apache.hadoop.mapreduce.{Job,Mapper,Reducer}
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import com.twitter.json.Json

class TweetsToHbaseMapper extends Mapper[Object,Text,NullWritable,Writable] {
  implicit def stringToByteArray(s: String): Array[Byte] = Bytes.toBytes(s)
  type JsonObject = Map[Any,Any]

  override def map(key: Object, value: Text, context: Mapper[Object,Text,NullWritable,Writable]#Context) = {
    try {
      val tweet = Json.parse(value.toString.trim).asInstanceOf[JsonObject]
      val put = new Put(tweet("id_str").asInstanceOf[String])
      put.add("raw", "user.id_str", tweet("user").asInstanceOf[JsonObject]("id_str").asInstanceOf[String])
      put.add("raw", "text", tweet("text").asInstanceOf[String])

      context.write(NullWritable.get, put)
    } catch { 
      case e: Exception => false // Don't care
    }
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

object tweetsToHbase {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create
    conf.set(TableOutputFormat.OUTPUT_TABLE, "randomdata")

    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 1) {
      println("Usage: <in>")
      return
    }

    val job = new Job(conf, "Tweets to HBase: " + otherArgs(0))
    job.setJarByClass(classOf[TweetsToHbaseMapper])
    FileInputFormat.setInputPaths(job, new Path(otherArgs(0)))
    job.setMapperClass(classOf[TweetsToHbaseMapper])
    job.setNumReduceTasks(0)
    job.setOutputFormatClass(classOf[TableOutputFormat[Array[Byte]]])

    job.waitForCompletion(true)
  }
}
