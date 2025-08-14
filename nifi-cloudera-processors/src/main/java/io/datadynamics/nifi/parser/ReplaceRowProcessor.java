package io.datadynamics.nifi.parser;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

public class ReplaceRowProcessor extends AbstractRowProcessor {

	private final String lineSeparator;
	private final String columnSeparator;
	private final Writer writer;
	private final String refinedLineDelimiter;
	private final AtomicLong count;
	private int initColumnCount = -1;

	public ReplaceRowProcessor(String lineSeparator, String columnSeparator, Writer writer, String refinedLineDelimiter, AtomicLong count) {
		this.lineSeparator = lineSeparator;
		this.columnSeparator = columnSeparator;
		this.writer = writer;
		this.refinedLineDelimiter = refinedLineDelimiter;
		this.count = count;
	}

	@Override
	public void rowProcessed(String[] rows, ParsingContext context) {
		if (initColumnCount == -1) {
			initColumnCount = rows.length;
		} else {
			if (initColumnCount != rows.length) {
				throw new IllegalStateException(String.format("컬럼 개수가 일치하지 않습니다. 초기 컬럼 개수: %s, 현재 컬럼 개수: %s", initColumnCount, rows.length));
			}
		}

		count.incrementAndGet();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < rows.length; i++) {
			sb.append(rows[i].replace("\r\n", " ").replace("\n", " "));
			if (i < rows.length - 1) {
				sb.append(columnSeparator);
			}
		}
		sb.append(lineSeparator);
		try {
			String rowString = sb.toString();
			System.out.println(rowString);
			writer.write(rowString);
		} catch (IOException e) {
			throw new RuntimeException("변환한 파일의 내용을 저장할 수 없습니다. 원인: " + e.getMessage(), e);
		}
	}
}
