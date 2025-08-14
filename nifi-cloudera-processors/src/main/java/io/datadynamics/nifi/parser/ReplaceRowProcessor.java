package io.datadynamics.nifi.parser;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

import static io.datadynamics.nifi.parser.MultilineCsvParser.COLUMN_SEP;

public class ReplaceRowProcessor extends AbstractRowProcessor {

	private final String inLineSep;
	private final String inColSep;
	private final String outLineSep;
	private final String outColSep;
	private final Writer writer;
	private final String refinedLineDelimiter;
	private final AtomicLong rowCount;
	private final int columnCount;
	private final boolean includeColumnSepAtLastColumn;
	private final int fixedSizeOfColumn;

	public ReplaceRowProcessor(String inLineSep, String inColSep,
	                           String outLineSep, String outColSep,
	                           Writer writer,
	                           String refinedLineDelimiter,
	                           AtomicLong rowCount,
	                           int columnCount,
	                           boolean includeColumnSepAtLastColumn,
	                           int fixedSizeOfColumn) {
		this.inLineSep = inLineSep;
		this.inColSep = inColSep;
		this.outLineSep = outLineSep;
		this.outColSep = outColSep;
		this.writer = writer;
		this.refinedLineDelimiter = refinedLineDelimiter;
		this.rowCount = rowCount;
		this.includeColumnSepAtLastColumn = includeColumnSepAtLastColumn;
		this.fixedSizeOfColumn = fixedSizeOfColumn;

		// CSV 파일과 다르게 마지막 컬럼 뒤에 컬럼 구분자가 오면 CSV 파서는 구분자 다음도 컬럼으로 인지하므로
		// 컬럼의 총 개수는 +1을 해야 합니다.
		if (includeColumnSepAtLastColumn) {
			this.columnCount = columnCount + 1;
		} else {
			this.columnCount = columnCount;
		}
	}

	@Override
	public void rowProcessed(String[] rows, ParsingContext context) {
		// 컬럼 카운트를 지정하면 컬럼의 개수를 검증합니다.
		if (columnCount > 0) {
			if (columnCount != rows.length) {
				throw new IllegalStateException(String.format("컬럼 개수가 일치하지 않습니다. 초기 컬럼 개수: %s, 현재 컬럼 개수: %s", includeColumnSepAtLastColumn ? columnCount - 1 : columnCount, includeColumnSepAtLastColumn ? rows.length - 1 : rows.length));
			}
		}

		rowCount.incrementAndGet();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < rows.length; i++) {
			if (fixedSizeOfColumn > 0) {
				// 성능을 고려하여 컬럼당 1회, 검증 조건이 켜 있는 경우에만 실행하도록 함.
				int length = rows[i].toCharArray().length;
				if (length != fixedSizeOfColumn) {
					throw new IllegalStateException(String.format("컬럼의 길이가 일치하지 않습니다. 검증할 컬럼의 길이: %s, 현재 컬럼의 길이: %s", fixedSizeOfColumn, length));
				}
			}

			// 컬럼의 크기가 고정이 아닌 가변일 때
			sb.append(rows[i].replace("\r\n", " ").replace("\n", " ").replace("" + COLUMN_SEP, inColSep));
			if (i < rows.length - 1) {
				sb.append(outColSep);
			}
		}
		sb.append(outLineSep);
		try {
			String rowString = sb.toString();
			writer.write(rowString);
		} catch (IOException e) {
			throw new RuntimeException("변환한 파일의 내용을 저장할 수 없습니다. 원인: " + e.getMessage(), e);
		}
	}
}
