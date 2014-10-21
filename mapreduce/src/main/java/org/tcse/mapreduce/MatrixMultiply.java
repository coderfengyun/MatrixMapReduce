package org.tcse.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiply {
	private static final int MATRIX_I = 4;
	private static final int MATRIX_J = 3;
	private static final int MATRIX_K = 2;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static final String CONTROL_I = "\u0009";

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String pathName = ((FileSplit) reporter.getInputSplit()).getPath()
					.toString();
			if (pathName.contains("lab_bigmmult_a")) {
				collectToOutput(output, value, true);
			} else if (pathName.contains("lab_bigmmult_b")) {
				collectToOutput(output, value, false);
			} else {
				// not my file
			}
		}

		void collectToOutput(OutputCollector<Text, Text> output, Text line,
				boolean isA) throws IOException {
			String[] values = extractValues(line);
			if (values == null) {
				return;
			}
			String rowIndex = values[0], columnIndex = values[1], elementValue = values[2];
			if (isA) {
				for (int k = 0; k < MATRIX_K; k++) {
					output.collect(new Text(rowIndex + CONTROL_I + k),
							new Text("a#" + columnIndex + "#" + elementValue));
				}
			} else {
				for (int i = 0; i < MATRIX_I; i++) {
					output.collect(new Text(i + CONTROL_I + columnIndex),
							new Text("b#" + rowIndex + "#" + elementValue));
				}
			}
		}

		private String[] extractValues(Text value) {
			String line = value.toString();
			if (line == null || line.isEmpty()) {
				return null;
			}
			String[] values = line.split(CONTROL_I);
			if (values.length < 3) {
				return null;
			}
			return values;
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> outputCollector, Reporter reporter)
				throws IOException {
			int[] rowValuesOfA = initWithZero(MATRIX_J), columnValuesOfB = initWithZero(MATRIX_J);
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
			outputCollector.collect(key,
					multiplyAndSum(rowValuesOfA, columnValuesOfB));
		}

		private Text multiplyAndSum(int[] rowValuesOfA, int[] columnValuesOfB) {
			int result = 0;
			for (int i = 0; i < columnValuesOfB.length; i++) {
				result += rowValuesOfA[i] * columnValuesOfB[i];
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