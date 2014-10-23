package org.tcse.mapreduce;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class Test_GenerateLine {

	@Test
	public void test_GenerateLine() throws IOException {
		ConvertMatrix2SparseAndEnlarge converter = new ConvertMatrix2SparseAndEnlarge(
				"source" + File.separator + "sourceMatrix.csv", 14, 2);
		converter.generateLineColumnSplittedByTab(0, 0, 1);
		converter.doConvert();
	}

	@Test
	public void test_GenerateLine_RowCount_3() throws IOException {
		ConvertMatrix2SparseAndEnlarge converter = new ConvertMatrix2SparseAndEnlarge(
				"source" + File.separator + "3lineMatrix.csv", 3, 2);
		converter.doConvert();
	}
}
