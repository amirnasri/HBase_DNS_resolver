package dnsTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * The bulk data loader main class. This class can be used to bulk-load 
 * an input file in hdfs into HBase. 
 * args[0]: hdfs input path
 * args[1]: hdfs output path
 * args[2]: HBase table name
 * 
 */

public class BulkLoader {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("hbase.table.name", args[2]);

		// Load hbase-site.xml
		HBaseConfiguration.addHbaseResources(conf);

		Job job = new Job(conf, "HBase bulk-load");
		job.setJarByClass(BulkLoaderMapper.class);

		job.setMapperClass(BulkLoaderMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		job.setInputFormatClass(TextInputFormat.class);

		HTable hTable = new HTable(conf, args[2]);

		// Auto configure partitioner and reducer.
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		// Load generated HFiles into HBase table.
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(new Path(args[1]), hTable);
	}
}