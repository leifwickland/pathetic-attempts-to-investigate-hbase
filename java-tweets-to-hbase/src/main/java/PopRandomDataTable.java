import java.io.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class PopRandomDataTable {
  public static class Map extends Mapper<LongWritable, Text, NullWritable, Writable> {

    @Override
    protected void map(LongWritable offset, Text input, Context context) throws IOException, InterruptedException {
      // my input is in JSON format, in other applications, you might be splitting a line of text or any of Hadoop's writable formats
      //RichArticle art = new RichArticle(input.toString());
      // RichArticles are able to output a good HBase row key, consisting of a timestamp and a unique identifier to prevent collisions. All keys in HBase are byte arrays.
      //byte[] rowId = art.getRowId();
      // We output multiple operations for each row
      //if (art.getTitle() != null) {
      //  Put put = new Put(rowId);
      //  put.add(Bytes.toBytes("content"), Bytes.toBytes("title"), Bytes.toBytes(art.getTitle()));
      //  context.write(NullWritable.get(), put);
      //}
      //if (art.getBody() != null) {
      //  Put put = new Put(rowId);
      //  put.add(Bytes.toBytes("content"), Bytes.toBytes("body"), Bytes.toBytes(art.getBody()));
      //  context.write(NullWritable.get(), put);
      //}
      Put put = new Put(Bytes.toBytes(input.toString()));
      put.add(Bytes.toBytes("raw"), Bytes.toBytes("b"), Bytes.toBytes(input.toString()));
      context.write(NullWritable.get(), put);
    }
  }

  public static void main(String args[]) throws Exception {
    //Configuration conf = new Configuration();
    Configuration conf = HBaseConfiguration.create();
    conf.set(TableOutputFormat.OUTPUT_TABLE, "randomdata");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 1) {
      System.out.println("Usage: <in>");
      return;
    }
    String input = otherArgs[0];
    Job job = new Job(conf, "Populate RandomData Table with " + input);
    // Input is just text files in HDFS
    FileInputFormat.addInputPath(job, new Path(input));
    job.setJarByClass(PopRandomDataTable.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);
    //HBaseConfiguration.addHbaseResources(job.getConfiguration());
    // Output is to the table output format, and we set the table we want
    job.setOutputFormatClass(TableOutputFormat.class);
    job.waitForCompletion(true);
  }
}
