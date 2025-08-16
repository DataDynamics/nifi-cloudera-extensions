package io.datadynamics.nifi.parser;


import shaded.com.univocity.parsers.csv.CsvFormat;
import shaded.com.univocity.parsers.csv.CsvParser;
import shaded.com.univocity.parsers.csv.CsvParserSettings;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"cloudera", "csv", "parse", "delimiter", "multi-character"})
@CapabilityDescription("라인과 컬럼 구분자가 모두 다문자 문자열이면서 2바이트 이상인 CSV 파일을 파싱하고, 컬럼값에 라인 구분자가 포함된 경우 Space 문자로 변경합니다. " +
        "선택적으로 헤더 행과 Quote을 지원하며(RFC4180 유사: 따옴표 문자는 두 번 연속으로 사용하여 이스케이프), 하나의 FlowFile을 출력합니다.")
@WritesAttributes({
        @WritesAttribute(attribute = "parsecsv.record.count", description = "파싱한 레코드 수(헤더 제외)"),
        @WritesAttribute(attribute = "parsecsv.header.present", description = "첫 번째 행을 헤더로 처리했다면 true"),
        @WritesAttribute(attribute = "parsecsv.input.line.delimiter", description = "입력 라인 구분자(이스케이프 해제 후)"),
        @WritesAttribute(attribute = "parsecsv.input.column.delimiter", description = "입력 컬럼 구분자(이스케이프 해제 후)"),
        @WritesAttribute(attribute = "parsecsv.output.line.delimiter", description = "출력 라인 구분자(이스케이프 해제 후)"),
        @WritesAttribute(attribute = "parsecsv.output.column.delimiter", description = "출력 컬럼 구분자(이스케이프 해제 후)"),
        @WritesAttribute(attribute = "mime.type", description = "MIME Type")
})
public class MultilineCsvParser extends AbstractProcessor {

    // ---- Constants ----

    public static final char RECORD_SEP = '\u001E'; // RS (Record Separator)
    public static final char COLUMN_SEP = '\u001F'; // US (Unit Separator)

    // ---- Input Delimiters ----
    public static final PropertyDescriptor LINE_DELIMITER = new PropertyDescriptor.Builder()
            .name("입력 파일의 라인 구분자")
            .description("입력 파일의 멀티 문자 기반의 라인 구분자. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("@@\\n")
            .build();

    public static final PropertyDescriptor COLUMN_DELIMITER = new PropertyDescriptor.Builder()
            .name("입력 파일의 컬럼 구분자")
            .description("입력 파일의 멀티 문자 기반의 컬럼 구분자. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("^|")
            .build();

    // ---- Quoting & Header ----
    public static final PropertyDescriptor QUOTE_CHAR = new PropertyDescriptor.Builder()
            .name("Quote 문자")
            .description("옵션으로 사용하는 컬럼의 인용 문자. 빈값으로 놔두면 적용하지 않습니다. " +
                    "만약에 설정한다면 (e.g. '\"'), 인용 문자로 감싸있는 필드 내부의 구분자는 무시합니다. 중복으로 (\"\") 사용하면 이스케이프 처리합니다.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new SingleCharAfterUnescapeValidator())
            .defaultValue("\"")
            .build();

    public static final PropertyDescriptor HAS_HEADER = new PropertyDescriptor.Builder()
            .name("헤더 존재 여부")
            .description("첫번째 ROW를 헤더로 처리합니다. 이 옵션을 활성화 하면 첫번째 ROW는 출력하지 않습니다.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ---- Charsets ----
    public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("입력 파일을 문자셋")
            .description("입력 파일을 디코딩시 사용할 문자셋. 예: CP949 (a.k.a. MS949).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("CP949") // 기본: CP949
            .build();

    public static final PropertyDescriptor OUTPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("출력 파일의 문자셋")
            .description("출력 파일의 인코딩 문자셋을 지정합니다. 예: UTF-8.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name()) // 기본: UTF-8
            .build();

    // ---- Output Delimiters ----
    public static final PropertyDescriptor OUTPUT_LINE_DELIMITER = new PropertyDescriptor.Builder()
            .name("출력 파일의 라인 구분자")
            .description("출력 파일을 멀티 문자 기반 라인 구분자를 지정합니다. 멀티 캐릭터를 지원합니다. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\\n")
            .build();

    public static final PropertyDescriptor OUTPUT_COLUMN_DELIMITER = new PropertyDescriptor.Builder()
            .name("출력 파일의 컬럼 구분자")
            .description("출력 파일의 멀티 문자 기반 컬럼 구분자를 지정합니다. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(",")
            .build();

    // ---- ETC ----
    public static final PropertyDescriptor COLUMN_COUNT = new PropertyDescriptor.Builder()
            .name("컬럼 카운트")
            .description("컬럼 카운트를 검증합니다. 이 값을 0보다 큰 값을 지정하면 CSV 파싱후 컬럼의 개수를 검증합니다. " +
                    "지정한 개수와 같지 않은 경우 더이상 처리하지 않습니다.")
            .required(false)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_COLUMN_SEP_AT_LAST_COLUMN = new PropertyDescriptor.Builder()
            .name("마지막 컬럼에도 컬럼 구분자 존재 여부")
            .description("CSV의 경우 마지막 컬럼 뒤에 컬럼 구분자가 없으나 이 옵션을 체크하면 컬럼 구분자가 포함된 것으로 간주하여, " +
                    "'컬럼 카운트'에서 설정한 값에서 1개를 빼서 검증합니다.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIXED_SIZE_COLUMN = new PropertyDescriptor.Builder()
            .name("컬럼의 크기가 고정 크기시 컬럼의 문자수")
            .description("컬럼의 크기가 고정 크기로 되어 있는 컬럼의 경우 컬럼의 문자수를 검증합니다. " +
                    "단, UTF로 가정하지 않고 Character의 개수를 검증하도록 합니다.")
            .required(false)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SKIP_EMPTY_LINE = new PropertyDescriptor.Builder()
            .name("빈 라인 건너뛰기")
            .description("빈 라인은 처리하지 않고 SKIP합니다.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ---- Relationships ----
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("성공시 변환한 FlowFile을 전달")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("원본 입력 FlowFile을 전달")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("파싱 실패시 원본 FlowFile을 전달")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final org.apache.nifi.processor.ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(LINE_DELIMITER);
        descriptors.add(COLUMN_DELIMITER);
        descriptors.add(QUOTE_CHAR);
        descriptors.add(HAS_HEADER);
        descriptors.add(COLUMN_COUNT);
        descriptors.add(INCLUDE_COLUMN_SEP_AT_LAST_COLUMN);
        descriptors.add(FIXED_SIZE_COLUMN);
        descriptors.add(SKIP_EMPTY_LINE);
        descriptors.add(INPUT_CHARACTER_SET);
        descriptors.add(OUTPUT_CHARACTER_SET);
        descriptors.add(OUTPUT_LINE_DELIMITER);
        descriptors.add(OUTPUT_COLUMN_DELIMITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog log = getLogger();

        // Upstream의 FlowFile을 확인합니다. 없으면 더이상 처리할 수 없습니다.
        FlowFile ff = session.get();
        if (ff == null) return;

        // 사용자가 입력한 파라미터 값들
        final String lineDelimRaw = context.getProperty(LINE_DELIMITER).evaluateAttributeExpressions(ff).getValue();
        final String colDelimRaw = context.getProperty(COLUMN_DELIMITER).evaluateAttributeExpressions(ff).getValue();
        final String quoteRaw = context.getProperty(QUOTE_CHAR).evaluateAttributeExpressions(ff).getValue();
        final boolean hasHeader = context.getProperty(HAS_HEADER).evaluateAttributeExpressions(ff).asBoolean();
        final int columnCount = context.getProperty(COLUMN_COUNT).evaluateAttributeExpressions(ff).asInteger();
        final boolean includeColumnSepAtLastColumn = context.getProperty(INCLUDE_COLUMN_SEP_AT_LAST_COLUMN).evaluateAttributeExpressions(ff).asBoolean();
        final boolean skipEmptyLine = context.getProperty(SKIP_EMPTY_LINE).evaluateAttributeExpressions(ff).asBoolean();
        final int fixedSizeOfColumn = context.getProperty(FIXED_SIZE_COLUMN).evaluateAttributeExpressions(ff).asInteger();
        final Charset inCharset = Charset.forName(context.getProperty(INPUT_CHARACTER_SET).evaluateAttributeExpressions(ff).getValue());
        final Charset outCharset = Charset.forName(context.getProperty(OUTPUT_CHARACTER_SET).evaluateAttributeExpressions(ff).getValue());
        final String outLineRaw = context.getProperty(OUTPUT_LINE_DELIMITER).evaluateAttributeExpressions(ff).getValue();
        final String outColRaw = context.getProperty(OUTPUT_COLUMN_DELIMITER).evaluateAttributeExpressions(ff).getValue();

        // 입력한 값들에 대한 escape를 처리합니다.
        final String lineDelim = unescape(lineDelimRaw);
        final String colDelim = unescape(colDelimRaw);
        final Character quoteChar = (quoteRaw == null || quoteRaw.isEmpty()) ? null : unescape(quoteRaw).charAt(0);
        final String outLineDelim = unescape(outLineRaw);
        final String outColDelim = unescape(outColRaw);

        final AtomicLong rowCounter = new AtomicLong(0);

        // Downstream으로 전달할 FlowFile을 생성합니다.
        FlowFile out = session.create(ff);

        // CSV Parser를 설정합니다.
        CsvParserSettings settings = new CsvParserSettings();
        settings.setSkipEmptyLines(skipEmptyLine);          // 빈 레코드도 유지 (필요 시 조절)
        settings.setHeaderExtractionEnabled(hasHeader);     // 헤더가 없다고 가정 (있다면 true)
        settings.setNullValue("");                          // null 대신 빈문자열
        settings.setEmptyValue("");                         // 빈 필드 값 보존

        // CSV Parser의 Quote를 설정합니다.
        CsvFormat format = settings.getFormat();
        if (quoteChar != null) {
            format.setQuote(quoteChar);                     // 기본 CSV 따옴표
            format.setQuoteEscape('"');                     // CSV 이스케이프 규칙 ("")
        }

        // CSV Parser가 기본으로 2개의 Delmiter를 지원하나 사용자가 3개 이상을 지정하는 경우 처리할 수 없으므로
        // 우선적으로 CSV Parser가 동작하도록 특수 문자로 구성한 라인 및 컬럼 구분자를 설정합니다.
        format.setLineSeparator(String.valueOf(RECORD_SEP));        // 레코드 구분자(단일문자)
        format.setDelimiter(COLUMN_SEP);                            // 컬럼 구분자(단일문자)

        try (final InputStream in = session.read(ff)) {
            // Upstream에서 전달받은 FlowFile을 파싱해서 Downstream으로 전달하기 위한 Row Processor를 생성하고 처리합니다.
            out = session.write(out, outStream -> {
                try (OutputStreamWriter writer = new OutputStreamWriter(outStream, outCharset)) {
                    try (Reader base = new BufferedReader(new InputStreamReader(in, inCharset), 8192);
                         Reader reader = new MultiDelimiterTranslatingReader(base, lineDelim, RECORD_SEP, colDelim, COLUMN_SEP)) {

                        // CSV Parser를 실행합니다. 실제 처리는 RowProcessor를 사용합니다.
                        // CSV Parser가 파싱한 컬럼을 특수하게 처리하고자 하는 경우 Row Processor를 구현하여 적용하도록 합니다.
                        NewlineToSpaceConverter rowProcessor = new NewlineToSpaceConverter(colDelim, outLineDelim, outColDelim, writer, rowCounter, columnCount, includeColumnSepAtLastColumn, fixedSizeOfColumn, log);
                        settings.setProcessor(rowProcessor);

                        CsvParser parser = new CsvParser(settings);
                        parser.parse(reader);
                    }
                } catch (IOException e) {
                    String msg = String.format("FlowFile을 읽어서 문자열을 Replace 처리하여 저장할 수 없습니다. 원인: %s", e.getMessage());
                    log.warn(msg, e);
                    throw new ProcessException(msg, e);
                }
            });

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("parsecsv.record.count", String.valueOf(rowCounter.get()));
            attrs.put("parsecsv.header.present", String.valueOf(hasHeader));
            attrs.put("parsecsv.input.line.delimiter", printable(lineDelim));
            attrs.put("parsecsv.input.column.delimiter", printable(colDelim));
            attrs.put("parsecsv.output.line.delimiter", printable(outLineDelim));
            attrs.put("parsecsv.output.column.delimiter", printable(outColDelim));
            attrs.put("mime.type", "text/plain; charset=" + outCharset.name());

            log.info("FlowFile을 downstream으로 전달합니다. FlowFile의 attributes: {}", attrs);

            out = session.putAllAttributes(out, attrs);
            session.transfer(out, REL_SUCCESS);
            session.transfer(ff, REL_ORIGINAL);
        } catch (Exception e) {
            log.error("CSV 파일을 파싱할 수 없습니다. 원인: {}", e.getMessage(), e);
            session.remove(out);

            ff = session.putAttribute(ff, "parsecsv.error", String.valueOf(e));
            session.transfer(ff, REL_FAILURE);
        }
    }

    // ---- Helpers ----
    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); ) {
            char c = s.charAt(i++);
            if (c == '\\' && i < s.length()) {
                char n = s.charAt(i++);
                switch (n) {
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    case 'u':
                        if (i + 3 < s.length()) {
                            String hex = s.substring(i, i + 4);
                            sb.append((char) Integer.parseInt(hex, 16));
                            i += 4;
                        } else {
                            sb.append("\\u");
                        }
                        break;
                    default:
                        sb.append(n);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Downstream으로 전달할 FlowFile의 Attribute에 들어가는 문자열을 출력 가능한 형태로 변환합니다.
     *
     * @param value 문자열
     * @return 변환한 문자열
     */
    private static String printable(String value) {
        return value.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    /**
     * NiFi Processor 설정 화면에서 사용자가 입력한 값이 최소 1개의 문자를 충족하는지 검증하는 Validator.
     */
    private static class SingleCharAfterUnescapeValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (input == null || input.isEmpty()) {
                return new ValidationResult.Builder().valid(true).subject(subject).input(input).explanation("empty allowed").build();
            }
            String u = unescape(input);
            boolean ok = u.length() == 1;
            return new ValidationResult.Builder()
                    .valid(ok)
                    .subject(subject)
                    .input(input)
                    .explanation(ok ? "OK" : "Unescape 후에 반드시 1개의 문자이어야 합니다.")
                    .build();
        }
    }

}