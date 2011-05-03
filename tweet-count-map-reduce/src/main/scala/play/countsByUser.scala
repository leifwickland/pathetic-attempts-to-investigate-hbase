package play

// The following was largely stolen from https://github.com/cageface/scala-hadoop-example/blob/master/WordCount.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text,IntWritable}
import org.apache.hadoop.mapreduce.{Job,Mapper,Reducer}
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._
import scala.collection.mutable.Queue

import com.twitter.json.Json

class CountsByUserMapper extends Mapper[Object,Text,Text,IntWritable] {
  implicit def hadoopTextToString(t: Text): String = t.toString
  type JsonObject = Map[Any,Any]
  val one = new IntWritable(1)
  val word = new Text

  override def map(key: Object, value: Text, context: Mapper[Object,Text,Text,IntWritable]#Context) = {
    try {
      val tweet = Json.parse(value).asInstanceOf[JsonObject]
      word.set(getStringFromJson(tweet, Queue("user", "id_str")))
      context.write(word, one)
    } catch {
      case e: Exception => false // There's bad JSON in there.  Don't care.
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

class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val value = new IntWritable(values.foldLeft(0) { (sum,i) => sum + i.get })
    context.write(key, value)
  }
}

object countsByUser {
  def main(args: Array[String]) {
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: wordcount <in> <out>")
      return
    }
    val job = new Job(conf, "counts by user")
    job.setJarByClass(classOf[CountsByUserMapper])
    job.setMapperClass(classOf[MultithreadedMapper[Object,Text,Text,IntWritable]])
    MultithreadedMapper.setMapperClass(job, classOf[CountsByUserMapper])
    MultithreadedMapper.setNumberOfThreads(job, 4)
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))
    job.waitForCompletion(true)
  }
}
