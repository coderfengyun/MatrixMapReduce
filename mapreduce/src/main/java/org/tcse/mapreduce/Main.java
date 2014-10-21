package org.tcse.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.tcse.mapreduce.MatrixMultiply.Map;
import org.tcse.mapreduce.MatrixMultiply.Reduce;

/**
 * Hello world!
 *
 */
public class Main {
	public static void main(String[] args) throws Exception {
		String pathNameOfA = "hdfs://133.133.134.188:9000/electric-experiment/lab_bigmmult_a.txt";
		String pathNameOfB = "hdfs://133.133.134.188:9000/electric-experiment/lab_bigmmult_b.txt";
		String output = "hdfs://133.133.134.188:9000/electric-experiment/lab_bigmmult_c.txt";

		JobConf conf = new JobConf(MatrixMultiply.class);
		conf.setJobName("WordCount");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(pathNameOfA), new Path(pathNameOfB));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		JobClient.runJob(conf);
		System.exit(0);
	}
}
