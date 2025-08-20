package io.datadynamics.nifi.parser;

import com.google.common.base.Joiner;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import shaded.com.univocity.parsers.common.ParsingContext;
import shaded.com.univocity.parsers.common.processor.AbstractRowProcessor;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static io.datadynamics.nifi.parser.MultilineCsvParser.COLUMN_SEP;

public class NewlineToSpaceConverter extends AbstractRowProcessor {

	private final ComponentLog log;
	private final AtomicLong errorCounter;
	private final boolean testMode;
	private final String inColSep;
	private final String outLineSep;
	private final String outColSep;
	private final Writer writer;
	private final AtomicLong rowCounter;
	private final int columnCount;
	private int columnCountForValidation;
	private int lastIndex;
	private final boolean includeColumnSepAtLastColumn;

	/**
	 * Parsing ROW를 처리하는 Processor.
	 *
	 * @param inColSep                     입력 컬럼 구분자
	 * @param outLineSep                   출력 라인 구분자
	 * @param outColSep                    출력 컬럼 구분자
	 * @param writer                       Writer
	 * @param rowCounter                   처리한 ROW Count
	 * @param includeColumnSepAtLastColumn 마지막 컬럼 뒤에 컬럼 구분자를 포함할지 여부
	 * @param log                          NiFi Component Logger
	 * @param errorCounter                 에러 ROW Count
	 * @param testMode                     테스트 모드로 동작할지 여부. 테스트 모드로 동작시 에러가 발생하더라도 로그를 기록으로 남기고 SKIP한다.
	 */
	public NewlineToSpaceConverter(String inColSep,
	                               String outLineSep, String outColSep,
	                               Writer writer,
	                               AtomicLong rowCounter,
	                               int columnCount,
	                               boolean includeColumnSepAtLastColumn,
	                               ComponentLog log,
	                               AtomicLong errorCounter, boolean testMode) {
		this.inColSep = inColSep;
		this.outLineSep = outLineSep;
		this.outColSep = outColSep;
		this.writer = writer;
		this.columnCount = columnCount;
		this.rowCounter = rowCounter;
		this.includeColumnSepAtLastColumn = includeColumnSepAtLastColumn;
		this.log = log;
		this.errorCounter = errorCounter;
		this.testMode = testMode;

		// CSV 파일과 다르게 마지막 컬럼 뒤에 컬럼 구분자가 오면 CSV 파서는 구분자 다음도 컬럼으로 인지하므로
		// 컬럼의 총 개수는 +1을 해야 합니다.
		if (includeColumnSepAtLastColumn) {
			this.lastIndex = columnCount - 1;
			this.columnCountForValidation = columnCount + 1;
			log.info(String.format("마지막 컬럼 뒤에 컬럼 구분자가 포함하는 경우 컬럼의 총 개수는 전체 개수의 +1이 됩니다. 설정한 컬럼의 개수: %s, 보정한 컬럼의 개수: %s", this.columnCount, this.columnCountForValidation));
		} else {
			this.columnCountForValidation = columnCount;
			log.info("설정한 컬럼의 개수: ", columnCountForValidation);
		}
	}

	@Override
	public void rowProcessed(String[] columns, ParsingContext context) {
		// 컬럼 카운트를 지정하면 컬럼의 개수를 검증합니다.
		if (columnCount != 0 && columnCountForValidation > 0) {
			if (columnCountForValidation != columns.length) {
				long c = errorCounter.incrementAndGet();
				StringBuilder builder = new StringBuilder();
				String msg = builder.append("컬럼의 개수를 검증하던 중 실패하였습니다. 마지막 컬럼 뒤에 컬럼 구분자가 포함하는 경우 컬럼의 총 개수는 전체 개수의 +1이 됩니다.").append("\n")
						.append("✔ 현재까지 에러 건수: ").append(c).append("\n")
						.append("✔ 설정한 컬럼의 개수: ").append(this.columnCount).append("\n")
						.append("✔ 보정한 컬럼의 개수: ").append(this.columnCountForValidation).append("\n")
						.append("✔ 파싱한 컬럼의 개수: ").append(columns.length).append("\n")
						.append("✔ 재조립한 ROW: ").append(Joiner.on(inColSep).join(columns)).append("\n")
						.append("✔ 재조립한 ROW(Hex): \n").append(PrettyHexDump.prettyHexDump(Joiner.on(inColSep).join(columns).getBytes())).append("\n").toString();
				System.out.println(msg);
				log.warn(msg);
				if (!this.testMode) {
					// 테스트 모드가 아니면 에러 메시지만 남기고 SKIP한다.
					throw new ProcessException(String.format("마지막 컬럼 뒤에 컬럼 구분자가 포함하는 경우 컬럼의 총 개수는 전체 개수의 +1이 됩니다. 설정한 컬럼의 개수: %s, 보정한 컬럼의 개수: %s, 파싱한 컬럼의 개수: %s, 재조립한 ROW: %s", this.columnCount, this.columnCountForValidation, columns.length, Joiner.on(inColSep).join(columns)));
				}
			}
		}

		String[] refinedColumns = null;
		StringBuilder sb = new StringBuilder();
		if (includeColumnSepAtLastColumn) {
			// 마지막에도 Column Delimiter가 있다면 마지막 컬럼은 제외하도록 한다.
			refinedColumns = Arrays.copyOfRange(columns, 0, Math.max(0, columns.length - 1));
		} else {
			refinedColumns = columns;
		}

		for (int i = 0; i < refinedColumns.length; i++) {
			// 컬럼의 크기가 고정이 아닌 가변일 때
			sb.append(refinedColumns[i].replace("\r\n", " ").replace("\n", " ").replace("" + COLUMN_SEP, inColSep));
			if (i < refinedColumns.length - 1) {
				sb.append(outColSep);
			}
		}

		// 출력을 위한 라인 구분자를 추가하고 ROW Counter를 +1 함
		sb.append(outLineSep);
		try {
			rowCounter.incrementAndGet();
			writer.write(sb.toString());
		} catch (IOException e) {
			throw new ProcessException("변환한 파일의 내용을 저장할 수 없습니다. 원인: " + e.getMessage(), e);
		}
	}
}
