package io.datadynamics.nifi.parser;

import com.google.common.base.Joiner;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import shaded.com.univocity.parsers.common.ParsingContext;
import shaded.com.univocity.parsers.common.processor.AbstractRowProcessor;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicLong;

import static io.datadynamics.nifi.parser.MultilineCsvParser.COLUMN_SEP;

public class NewlineToSpaceConverter extends AbstractRowProcessor {

    private final ComponentLog log;
    private final String inColSep;
    private final String outLineSep;
    private final String outColSep;
    private final Writer writer;
    private final AtomicLong rowCounter;
    private final int columnCountForValidation;
    private final boolean includeColumnSepAtLastColumn;
    private final int fixedSizeOfColumnForValidation;

    /**
     * Parsing ROW를 처리하는 Processor.
     *
     * @param inColSep                       입력 컬럼 구분자
     * @param outLineSep                     출력 라인 구분자
     * @param outColSep                      출력 컬럼 구분자
     * @param writer                         Writer
     * @param rowCounter                     처리한 ROW Count
     * @param columnCountForValidation       검증할 컬럼 개수 (컬럼 개수가 다른 경우 처리 중지)
     * @param includeColumnSepAtLastColumn   마지막 컬럼 뒤에 컬럼 구분자를 포함할지 여부
     * @param fixedSizeOfColumnForValidation 고정
     * @param log                            NiFi Component Logger
     */
    public NewlineToSpaceConverter(String inColSep,
                                   String outLineSep, String outColSep,
                                   Writer writer,
                                   AtomicLong rowCounter,
                                   int columnCountForValidation,
                                   boolean includeColumnSepAtLastColumn,
                                   int fixedSizeOfColumnForValidation, ComponentLog log) {
        this.inColSep = inColSep;
        this.outLineSep = outLineSep;
        this.outColSep = outColSep;
        this.writer = writer;
        this.rowCounter = rowCounter;
        this.includeColumnSepAtLastColumn = includeColumnSepAtLastColumn;
        this.fixedSizeOfColumnForValidation = fixedSizeOfColumnForValidation;
        this.log = log;

        // CSV 파일과 다르게 마지막 컬럼 뒤에 컬럼 구분자가 오면 CSV 파서는 구분자 다음도 컬럼으로 인지하므로
        // 컬럼의 총 개수는 +1을 해야 합니다.
        if (includeColumnSepAtLastColumn) {
            this.columnCountForValidation = columnCountForValidation + 1;
            log.info("마지막 컬럼 뒤에 컬럼 구분자가 포함하는 경우 컬럼의 총 개수는 전체 개수의 +1이 됩니다. 컬럼의 개수: ", columnCountForValidation);
        } else {
            this.columnCountForValidation = columnCountForValidation;
            log.info("컬럼의 총 개수: ", columnCountForValidation);
        }
    }

    @Override
    public void rowProcessed(String[] rows, ParsingContext context) {
        // 컬럼 카운트를 지정하면 컬럼의 개수를 검증합니다.
        if (columnCountForValidation > 0) {
            if (columnCountForValidation != rows.length) {
                log.warn(Joiner.on("").join(rows));
                throw new ProcessException(String.format("컬럼 개수가 일치하지 않습니다. 더이상 처리할 수 없습니다. 검증할 컬럼 개수: %s, 현재 컬럼 개수: %s", includeColumnSepAtLastColumn ? columnCountForValidation - 1 : columnCountForValidation, includeColumnSepAtLastColumn ? rows.length - 1 : rows.length));
            }
        }

        // 컬럼의 문자열 크기가 고정일때 지정한 컬럼의 크기와 같은지 확인하고 같지 않으면 예외를 발생하여 더이상 처리하지 않도록 함
        if (fixedSizeOfColumnForValidation > 0) {
            // 모든 컬럼의 값을 JOIN하여 길이를 확인합니다.
            int length = Joiner.on("").join(rows).getBytes().length;
            if (length != fixedSizeOfColumnForValidation) {
                throw new ProcessException(String.format("컬럼의 길이가 일치하지 않습니다. 더이상 처리할 수 없습니다. 검증할 컬럼의 길이: %s, 현재 컬럼의 길이: %s", fixedSizeOfColumnForValidation, length));
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.length; i++) {
            // 컬럼의 크기가 고정이 아닌 가변일 때
            sb.append(rows[i].replace("\r\n", " ").replace("\n", " ").replace("" + COLUMN_SEP, inColSep));
            if (i < rows.length - 1) {
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
