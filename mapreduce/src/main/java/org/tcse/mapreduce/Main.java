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
		JobConf conf = createConf();
		String fsDefaultName = conf.get("fs.default.name");
		String pathNameOfA = fsDefaultName + "/electric-experiment/lab_bigmmult_a.txt";
		String pathNameOfB = fsDefaultName + "/electric-experiment/lab_bigmmult_b.txt";
		String result = fsDefaultName + "/electric-experiment/lab_bigmmult_c.txt";
		FileInputFormat.setInputPaths(conf, new Path(pathNameOfA), new Path(pathNameOfB));
		FileOutputFormat.setOutputPath(conf, new Path(result));
		JobClient.runJob(conf);
		System.exit(0);
	}

	private static JobConf createConf() {
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
		return conf;
	}
}
