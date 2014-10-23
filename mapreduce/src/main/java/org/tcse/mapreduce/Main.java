package org.tcse.mapreduce;

import java.util.Date;

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
	static String SOURCE_FILE;

	public static void main(String[] args) throws Exception {
		if (!parseInputParameters(args)) {
			return;
		}
		JobConf conf = createMatrixMultiplyJob();
		String fsDefaultName = "hdfs://133.133.134.188:9000";
		String pathNameOfA = SOURCE_FILE;
		String result = fsDefaultName + "/electric-experiment/result"
				+ new Date().getTime() + ".txt";
		FileInputFormat.setInputPaths(conf, new Path(pathNameOfA));
		FileOutputFormat.setOutputPath(conf, new Path(result));
		JobClient.runJob(conf);
		System.exit(0);
	}

	private static boolean parseInputParameters(String[] args) {
		if (args.length < 4 || args[0] == "-help") {
			System.out
					.println("-row_count XX : show how many rows this matrix have");
			System.out.println("-source_path XX : this is hdfs's full path");
			return false;
		}
		if (!args[0].equalsIgnoreCase("-row_count")) {
			throw new RuntimeException("args[0] != -row_count!" + "args[0] = "
					+ args[0]);
		}
		MatrixMultiply.MATRIX_I = Integer.parseInt(args[1]);
		MatrixMultiply.MATRIX_J = MatrixMultiply.MATRIX_I;
		MatrixMultiply.MATRIX_K = MatrixMultiply.MATRIX_I;
		if (!args[2].equalsIgnoreCase("-source_path")) {
			throw new RuntimeException("args[0] != -source_path!");
		}
		SOURCE_FILE = args[3];
		return true;
	}

	private static JobConf createMatrixMultiplyJob() {
		JobConf conf = new JobConf(MatrixMultiply.class);
		conf.setJobName("MatrixMultiply");
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		return conf;
	}
}
