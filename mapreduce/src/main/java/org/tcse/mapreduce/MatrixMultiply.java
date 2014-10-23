package org.tcse.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MatrixMultiply {

	public static void main(String[] args) throws Exception {
		JobConf conf = createMatrixMultiplyJob();
		if (!parseInputParameters(args, conf)) {
			return;
		}
		JobClient.runJob(conf);
		System.exit(0);
	}

	private static boolean parseInputParameters(String[] args, JobConf jobConf) {
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
		jobConf.set("row_count", args[1]);
		if (!args[2].equalsIgnoreCase("-source_path")) {
			throw new RuntimeException("args[0] != -source_path!");
		}
		FileInputFormat.setInputPaths(jobConf, new Path(args[3]));
		String resultPath = "hdfs://133.133.134.188:9000"
				+ "/electric-experiment/result/"
				+ new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss")
						.format(new Date()) + ".txt";
		FileOutputFormat.setOutputPath(jobConf, new Path(resultPath));
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
		conf.setInt("mapred.reduce.tasks", 10);
		conf.setInt("mapred.map.tasks", 10);
		return conf;
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private final String TAB = "\t";
		private int ROW_COUNT = 0;

		@Override
		public void configure(JobConf job) {
			this.ROW_COUNT = job.getInt("row_count", 0);
			super.configure(job);
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			collectToOutput(output, value);
		}

		void collectToOutput(OutputCollector<Text, Text> output, Text line)
				throws IOException {
			String[] values = extractValues(line);
			if (values == null) {
				System.err.println("wrong values is " + line.toString());
				return;
			}
			String rowIndex = values[0], columnIndex = values[1], elementValue = values[2];
			for (int k = 0; k < this.ROW_COUNT; k++) {
				String key = rowIndex + TAB + k, value = "a#" + columnIndex
						+ "#" + elementValue;
				output.collect(new Text(key), new Text(value));
			}
			for (int i = 0; i < this.ROW_COUNT; i++) {
				String key = i + TAB + columnIndex, value = "b#" + rowIndex
						+ "#" + elementValue;
				output.collect(new Text(key), new Text(value));
			}
		}

		private String[] extractValues(Text value) {
			String line = value.toString();
			if (line == null || line.isEmpty()) {
				return null;
			}
			String[] values = line.split(TAB);
			if (values.length < 3) {
				return null;
			}
			return values;
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private int ROW_COUNT = 0;

		@Override
		public void configure(JobConf job) {
			this.ROW_COUNT = job.getInt("row_count", 0);
			super.configure(job);
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> outputCollector, Reporter reporter)
				throws IOException {
			int[] rowValuesOfA = initWithZero(this.ROW_COUNT), columnValuesOfB = initWithZero(this.ROW_COUNT);
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] valueLine = value.split("#");
				if (value.startsWith("a#")) {
					rowValuesOfA[Integer.parseInt(valueLine[1])] = Integer
							.parseInt(valueLine[2]);
				} else if (value.startsWith("b#")) {
					columnValuesOfB[Integer.parseInt(valueLine[1])] = Integer
							.parseInt(valueLine[2]);
				}
			}
			Text value = multiplyAndSum(rowValuesOfA, columnValuesOfB);
			if (value.toString().equals("0")) {
				// record the non-zero only, to construct the Sparse matrix
				return;
			}
			outputCollector.collect(key, value);
			System.out.println("collectResult : " + key.toString() + " -> "
					+ value.toString());
		}

		private Text multiplyAndSum(int[] rowValuesOfA, int[] columnValuesOfB) {
			int result = 0;
			for (int i = 0; i < columnValuesOfB.length; i++) {
				result = result | (rowValuesOfA[i] | columnValuesOfB[i]);
			}
			return new Text(Integer.toString(result));
		}

		private int[] initWithZero(int arrayLength) {
			int[] result = new int[arrayLength];
			Arrays.fill(result, 0);
			return result;
		}
	}

}