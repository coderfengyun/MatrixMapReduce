package org.tcse.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiply {
	static int MATRIX_I;
	static int MATRIX_J;
	static int MATRIX_K;

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// Tab
		private static final String TAB = "\t";

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// String pathName = ((FileSplit)
			// reporter.getInputSplit()).getPath()
			// .toString();
			// if (!pathName.contains(Main.SOURCE_FILE)) {
			// // not my file
			// return;
			// }
			collectToOutput(output, value, true);
			collectToOutput(output, value, false);
		}

		void collectToOutput(OutputCollector<Text, Text> output, Text line,
				boolean isA) throws IOException {
			System.out.println("line is : " + line.toString());
			String[] values = extractValues(line);
			if (values == null) {
				System.out.println("values is " + values);
				return;
			}
			String rowIndex = values[0], columnIndex = values[1], elementValue = values[2];
			if (isA) {
				for (int k = 0; k < MATRIX_K; k++) {
					String key = rowIndex + TAB + k, value = "a#" + columnIndex
							+ "#" + elementValue;
					output.collect(new Text(key), new Text(value));
					System.out.println(key + " -> " + value);
				}
			} else {
				for (int i = 0; i < MATRIX_I; i++) {
					String key = i + TAB + columnIndex, value = "b#" + rowIndex
							+ "#" + elementValue;
					output.collect(new Text(key), new Text(value));
					System.out.println(key + " -> " + value);
				}
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

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> outputCollector, Reporter reporter)
				throws IOException {
			int[] rowValuesOfA = initWithZero(MATRIX_J), columnValuesOfB = initWithZero(MATRIX_J);
			while (values.hasNext()) {
				String value = values.next().toString();
				System.out.println("currentKey is " + key
						+ "; currentValue is " + value);
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