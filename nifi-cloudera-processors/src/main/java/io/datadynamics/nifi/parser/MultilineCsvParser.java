package io.datadynamics.nifi.parser;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"csv", "multiline", "parser", "stream", "custom-delimiter"})
@CapabilityDescription("멀티라인 CSV를 커스텀 구분자/인코딩/Quote로 스트리밍 파싱 후 저장. " +
        "선행 N행 스킵, 그 다음 1행을 헤더로 읽어 출력의 첫 행으로 기록. " +
        "expectedColumns 불일치 시 실패. 정규식 미사용. ")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "csv.rows.skipped", description = "스킵된 레코드 수"),
        @WritesAttribute(attribute = "csv.rows.header", description = "헤더 기록 여부(0/1)"),
        @WritesAttribute(attribute = "csv.rows.data.in", description = "입력 데이터 레코드 수(헤더 제외)"),
        @WritesAttribute(attribute = "csv.rows.data.out", description = "출력 데이터 레코드 수"),
        @WritesAttribute(attribute = "csv.error", description = "실패 시 에러 메시지")
})
public class MultilineCsvParser extends AbstractProcessor {

    /* ===== Relationships ===== */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("변환 성공")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("변환 실패")
            .build();

    /* ===== Properties ===== */
    public static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Input Charset")
            .description("입력 파일 문자셋")
            .defaultValue("CP949")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUTPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Output Charset")
            .description("출력 파일 문자셋")
            .defaultValue("UTF-8")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Input Column Delimiter")
            .description("입력 컬럼 구분자(최소 2문자)")
            .defaultValue("^|")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Input Row Delimiter")
            .description("입력 행 구분자(최소 2문자, 예: \"@@\\n\")")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Output Column Delimiter")
            .description("출력 컬럼 구분자")
            .defaultValue("^|")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Output Row Delimiter")
            .description("출력 행 구분자")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_QUOTE = new PropertyDescriptor.Builder()
            .name("Input Quote")
            .description("입력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_QUOTE = new PropertyDescriptor.Builder()
            .name("Output Quote")
            .description("출력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue QM_NEVER = new AllowableValue("NEVER", "NEVER", "절대 Quote 사용 안 함");
    public static final AllowableValue QM_ALWAYS = new AllowableValue("ALWAYS", "ALWAYS", "항상 Quote");
    public static final AllowableValue QM_AS_NEEDED = new AllowableValue("AS_NEEDED", "AS_NEEDED", "필요 시에만 Quote");

    public static final PropertyDescriptor OUT_QUOTE_MODE = new PropertyDescriptor.Builder()
            .name("Output Quote Mode")
            .description("출력 Quote 전략")
            .required(true)
            .defaultValue(QM_NEVER.getValue())
            .allowableValues(QM_NEVER, QM_ALWAYS, QM_AS_NEEDED)
            .build();

    public static final PropertyDescriptor PRESERVE_INPUT_QUOTES = new PropertyDescriptor.Builder()
            .name("Preserve Input Quotes")
            .description("입력 Quote를 값에 보존할지 여부")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor EXPECTED_COLUMNS = new PropertyDescriptor.Builder()
            .name("Expected Columns")
            .description("기대한 컬럼 수(>0이면 강제, 다르면 실패)")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SKIP_LEADING_ROWS = new PropertyDescriptor.Builder()
            .name("Skip Leading Rows")
            .description("파싱 시작 전에 스킵할 레코드 수(Row Delimiter 단위)")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor READ_HEADER_AFTER_SKIP = new PropertyDescriptor.Builder()
            .name("Read Header After Skip Leading Rows")
            .description("스킵 직후 1행을 헤더로 읽어 출력 첫 행으로 기록")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> d = new ArrayList<>();
        d.add(INPUT_CHARSET);
        d.add(OUTPUT_CHARSET);
        d.add(IN_COL_DELIM);
        d.add(IN_ROW_DELIM);
        d.add(OUT_COL_DELIM);
        d.add(OUT_ROW_DELIM);
        d.add(IN_QUOTE);
        d.add(OUT_QUOTE);
        d.add(OUT_QUOTE_MODE);
        d.add(PRESERVE_INPUT_QUOTES);
        d.add(EXPECTED_COLUMNS);
        d.add(SKIP_LEADING_ROWS);
        d.add(READ_HEADER_AFTER_SKIP);
        descriptors = Collections.unmodifiableList(d);

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /* ===== 내부 출력 Writer ===== */
    private enum OutputQuoteMode {NEVER, ALWAYS, AS_NEEDED}

    private static final class CsvWriter implements Closeable, Flushable {
        private final java.io.Writer writer;
        private final String colDelim, rowDelim;
        private final char quote;
        private final OutputQuoteMode mode;

        CsvWriter(java.io.Writer writer, String colDelim, String rowDelim, char quote, OutputQuoteMode mode) {
            this.writer = new BufferedWriter(writer, 8192);
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
            this.mode = mode;
        }

        void writeRow(List<String> row) throws IOException {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) writer.write(colDelim);
                writeField(row.get(i));
            }
            writer.write(rowDelim);
        }

        private void writeField(String value) throws IOException {
            String v = (value == null) ? "" : value;
            boolean mustQuote = switch (mode) {
                case ALWAYS -> true;
                case NEVER -> false;
                default -> v.indexOf(quote) >= 0 || v.contains(colDelim) || v.contains(rowDelim);
            };
            if (!mustQuote) {
                writer.write(v);
                return;
            }
            String escaped = v.replace(String.valueOf(quote), String.valueOf(quote) + quote);
            writer.write(quote);
            writer.write(escaped);
            writer.write(quote);
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /* ===== 내부 입력 Parser (정규식 미사용) ===== */
    private static final class CsvParser implements Closeable {
        private final PushbackReader reader;
        private final String colDelim, rowDelim;
        private final char quote;
        private final boolean preserveInputQuotes;
        private final StringBuilder field = new StringBuilder();
        private final List<String> row = new ArrayList<>();
        private boolean inQuote = false, fieldStarted = false, eof = false;

        CsvParser(Reader r, String colDelim, String rowDelim, char quote, boolean preserveInputQuotes) {
            this.reader = new PushbackReader(new BufferedReader(r, 8192), 1);
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
            this.preserveInputQuotes = preserveInputQuotes;
        }

        List<String> readNextRow() throws IOException {
            if (eof) return null;
            field.setLength(0);
            row.clear();
            inQuote = false;
            fieldStarted = false;

            int ci;
            while ((ci = reader.read()) != -1) {
                char ch = (char) ci;

                if (!inQuote) {
                    if (!fieldStarted) {
                        fieldStarted = true;
                        if (ch == quote) {
                            inQuote = true;
                            if (preserveInputQuotes) field.append(ch);
                            continue;
                        }
                    }
                    field.append(ch);

                    if (endsWith(field, rowDelim)) {
                        trimTail(field, rowDelim.length());
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        return row;
                    }
                    if (endsWith(field, colDelim)) {
                        trimTail(field, colDelim.length());
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        continue;
                    }
                } else {
                    if (ch == quote) {
                        int next = reader.read();
                        if (next == -1) {
                            if (preserveInputQuotes) field.append(ch);
                            break;
                        }
                        char nch = (char) next;
                        if (nch == quote) {
                            if (preserveInputQuotes) field.append(ch).append(nch);
                            else field.append(ch);
                        } else {
                            if (preserveInputQuotes) field.append(ch);
                            inQuote = false;
                            reader.unread(nch);
                        }
                    } else {
                        field.append(ch);
                    }
                }
            }

            eof = true;
            if (field.length() > 0 || !row.isEmpty()) {
                row.add(field.toString());
                return row;
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        private static boolean endsWith(StringBuilder sb, String token) {
            int n = token.length(), m = sb.length();
            if (m < n) return false;
            for (int i = 0; i < n; i++) {
                if (sb.charAt(m - n + i) != token.charAt(i)) return false;
            }
            return true;
        }

        private static void trimTail(StringBuilder sb, int count) {
            sb.setLength(sb.length() - count);
        }
    }

    /* ===== StreamCallback ===== */
    private static final class TransformCallback implements StreamCallback {
        private final Charset inCs, outCs;
        private final String inCol, inRow, outCol, outRow;
        private final char inQuote, outQuote;
        private final OutputQuoteMode quoteMode;
        private final boolean preserveInputQuotes;
        private final int expectedColumns, skipLeadingRows;
        private final boolean readHeaderAfterSkip;
        private final RowProcessor rowProcessor;

        final AtomicLong rowsSkipped = new AtomicLong(0);
        final AtomicLong headerWritten = new AtomicLong(0);
        final AtomicLong rowsIn = new AtomicLong(0);
        final AtomicLong rowsOut = new AtomicLong(0);
        volatile String errorMessage = null;

        TransformCallback(Charset inCs, Charset outCs,
                          String inCol, String inRow, String outCol, String outRow,
                          char inQuote, char outQuote, OutputQuoteMode quoteMode,
                          boolean preserveInputQuotes,
                          int expectedColumns, int skipLeadingRows, boolean readHeaderAfterSkip,
                          RowProcessor rowProcessor) {
            this.inCs = inCs;
            this.outCs = outCs;
            this.inCol = inCol;
            this.inRow = inRow;
            this.outCol = outCol;
            this.outRow = outRow;
            this.inQuote = inQuote;
            this.outQuote = outQuote;
            this.quoteMode = quoteMode;
            this.preserveInputQuotes = preserveInputQuotes;
            this.expectedColumns = expectedColumns;
            this.skipLeadingRows = skipLeadingRows;
            this.readHeaderAfterSkip = readHeaderAfterSkip;
            this.rowProcessor = rowProcessor;
        }

        @Override
        public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
            try (Reader r = new InputStreamReader(rawIn, inCs);
                 CsvParser parser = new CsvParser(r, inCol, inRow, inQuote, preserveInputQuotes);
                 Writer w = new OutputStreamWriter(rawOut, outCs);
                 CsvWriter writer = new CsvWriter(w, outCol, outRow, outQuote, quoteMode)) {

                for (int i = 0; i < skipLeadingRows; i++) {
                    if (parser.readNextRow() == null) {
                        if (readHeaderAfterSkip) {
                            throw new ProcessException("EOF while skipping " + skipLeadingRows + " rows: no header present.");
                        } else {
                            writer.flush();
                            rowsSkipped.set(i);
                            return;
                        }
                    } else {
                        rowsSkipped.incrementAndGet();
                    }
                }

                if (readHeaderAfterSkip) {
                    List<String> header = parser.readNextRow();
                    if (header == null)
                        throw new ProcessException("No header after skipping " + skipLeadingRows + " rows.");
                    if (expectedColumns > 0 && header.size() != expectedColumns) {
                        throw new ProcessException("Header column count mismatch (expected=" + expectedColumns + ", actual=" + header.size() + "): " + header);
                    }
                    writer.writeRow(header);
                    headerWritten.set(1);
                }

                List<String> inRow;
                int dataIndex = 0;
                while ((inRow = parser.readNextRow()) != null) {
                    dataIndex++;
                    rowsIn.incrementAndGet();
                    if (expectedColumns > 0 && inRow.size() != expectedColumns) {
                        throw new ProcessException("Column count mismatch at data row " + dataIndex +
                                " (expected=" + expectedColumns + ", actual=" + inRow.size() + "): " + inRow);
                    }
                    List<String> outRowList = (rowProcessor != null) ? rowProcessor.process(inRow) : inRow;
                    writer.writeRow(outRowList);
                    rowsOut.incrementAndGet();
                }
                writer.flush();
            } catch (ProcessException e) {
                errorMessage = e.getMessage();
                throw e;
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog log = getLogger();

        FlowFile ff = session.get();
        if (ff == null) return;

        Map<String, String> orgAttributes = ff.getAttributes();

        final Charset inCs = Charset.forName(context.getProperty(INPUT_CHARSET).getValue());
        final Charset outCs = Charset.forName(context.getProperty(OUTPUT_CHARSET).getValue());
        final String inCol = context.getProperty(IN_COL_DELIM).getValue();
        final String inRow = context.getProperty(IN_ROW_DELIM).getValue();
        final String outCol = context.getProperty(OUT_COL_DELIM).getValue();
        final String outRow = context.getProperty(OUT_ROW_DELIM).getValue();

        final char inQuote = context.getProperty(IN_QUOTE).getValue().charAt(0);
        final char outQuote = context.getProperty(OUT_QUOTE).getValue().charAt(0);
        final OutputQuoteMode outMode = OutputQuoteMode.valueOf(context.getProperty(OUT_QUOTE_MODE).getValue());
        final boolean preserve = context.getProperty(PRESERVE_INPUT_QUOTES).asBoolean();

        final int expectedCols = context.getProperty(EXPECTED_COLUMNS).asInteger();
        final int skipRows = context.getProperty(SKIP_LEADING_ROWS).asInteger();
        final boolean readHeader = context.getProperty(READ_HEADER_AFTER_SKIP).asBoolean();

        // 구분자 길이 방어
        if (inCol.length() < 2 || inRow.length() < 2 || outCol.length() < 2 || outRow.length() < 2) {
            ff = session.putAttribute(ff, "csv.error", "Delimiters must be length >= 2");
            session.transfer(ff, REL_FAILURE);
            return;
        }

        // RowProcessor 인스턴스 준비(옵션)
        RowProcessor rp = new RowProcessor() {
            @Override
            public List<String> process(List<String> rows) {
                List<String> newList = new ArrayList<>(rows.size());
                for (String row : rows) {
                    newList.add(row.replace("\r\n", " ").replace("\n", " "));
                }
                return rows;
            }
        };

        final TransformCallback cb = new TransformCallback(
                inCs, outCs, inCol, inRow, outCol, outRow,
                inQuote, outQuote, outMode, preserve,
                expectedCols, skipRows, readHeader, rp
        );

        try {
            final Map<String, String> attrs = new HashMap<>(orgAttributes); // Upstream의 FlowFile Attributes 전체를 그대로 전달.
            attrs.put("parsecsv.input.charset", inCs.name());
            attrs.put("parsecsv.rows.skipped", String.valueOf(cb.rowsSkipped.get()));
            attrs.put("parsecsv.rows.header", String.valueOf(cb.headerWritten.get()));
            attrs.put("parsecsv.rows.data.in", String.valueOf(cb.rowsIn.get()));
            attrs.put("parsecsv.rows.data.out", String.valueOf(cb.rowsOut.get()));

            ff = session.write(ff, cb);
            session.putAllAttributes(ff, attrs);
            session.transfer(ff, REL_SUCCESS);
        } catch (ProcessException pe) {
            String err = (cb.errorMessage != null) ? cb.errorMessage : pe.getMessage();
            log.error("CSV parsing failed: {}", new Object[]{err}, pe);
            ff = session.putAttribute(ff, "parsecsv.error", err);
            session.transfer(ff, REL_FAILURE);
        }
    }
}