package org.tcse.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class ConvertMatrix2SparseAndEnlarge {
	private final File sourceFile;
	private final BufferedReader sourceReader;
	private final File targetFile;
	private final BufferedWriter targetWriter;
	private final int enlargeTimes;
	private final int rowCount;

	/**
	 * Pay attention that the source matrix should be a square, not only a
	 * normal matrix
	 * 
	 * @param inputFilePath
	 *            is where the tiny source matrix exist
	 * @param rowCount
	 *            is source matrix's row count
	 * @param enlargeTimes
	 *            is the enlarge times from tiny source matrix to target huge
	 *            matrix
	 * @throws IOException
	 */
	public ConvertMatrix2SparseAndEnlarge(String inputFilePath, int rowCount,
			int enlargeTimes) throws IOException {
		this.sourceFile = new File(inputFilePath);
		if (!this.sourceFile.exists()) {
			throw new RuntimeException("The inputFilePath -> " + inputFilePath
					+ " doesn't exist!");
		}
		this.sourceReader = new BufferedReader(new FileReader(this.sourceFile));
		if (enlargeTimes <= 0 || rowCount <= 0) {
			throw new RuntimeException(
					"The input enlargeTimes or rowCount <= 0");
		}
		this.enlargeTimes = enlargeTimes;
		this.rowCount = rowCount;
		this.targetFile = createTargetFile(this.sourceFile.getName(),
				enlargeTimes);
		this.targetWriter = new BufferedWriter(new FileWriter(this.targetFile));
	}

	private File createTargetFile(String inputFileName, int enlargeTimes) {
		File dir = new File("result");
		if (!dir.exists()) {
			dir.mkdirs();
		}
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("result");
		stringBuilder.append(File.separator);
		stringBuilder.append(inputFileName);
		stringBuilder.append("-");
		stringBuilder.append("Enlarged_By_");
		stringBuilder.append(enlargeTimes);
		stringBuilder.append("-");
		stringBuilder.append(new Date().getTime());
		stringBuilder.append(".txt");
		return new File(stringBuilder.toString());
	}

	public void doConvert() throws IOException {
		String line;
		try {
			for (int currentRowIndex = 0; (line = this.sourceReader.readLine()) != null
					&& currentRowIndex < this.rowCount; currentRowIndex++) {
				enlargeAndGenerateSparseLines(currentRowIndex, line);
			}
			this.targetWriter.flush();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			this.sourceReader.close();
			this.targetWriter.close();
		}

	}

	private void enlargeAndGenerateSparseLines(int currentRowIndex, String line)
			throws IOException {
		String[] values = line.split(",");
		if (values.length != this.rowCount) {
			throw new RuntimeException("This line with value : " + line
					+ ", size is not equals to ROW_COUNT");
		}
		for (int i = 0; i < this.rowCount; i++) {
			int val = Integer.parseInt(values[i]);
			if (val == 0) {
				continue;
			}
			// currentRowIndex, i, val -> currentRowIndx, i + k*RowCount, val ->
			// currentRowIndex + k*RowCount, i, val
			// k from 1 to enlargeTimes
			generateLineColumnSplittedByTab(currentRowIndex, i, val);
			for (int k = 1; k < this.enlargeTimes; k++) {
				generateLineColumnSplittedByTab(currentRowIndex, i + k
						* this.rowCount, val);
				generateLineColumnSplittedByTab(currentRowIndex + k
						* this.rowCount, i, val);
			}
		}
	}

	public String generateLineColumnSplittedByTab(int currentRowIndex, int i,
			int val) throws IOException {
		String result = currentRowIndex + "\t" + i + "\t" + val;
		System.out.println(result);
		this.targetWriter.write(result, 0, result.length());
		this.targetWriter.newLine();
		return result;
	}
}
