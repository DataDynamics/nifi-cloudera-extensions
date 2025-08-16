package io.datadynamics.nifi.parser;


import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
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
@CapabilityDescription("Parses CSV-like files where both the line and column delimiters may be multi-character strings. " +
		"Optionally supports a header row and quoting (RFC4180-like: quote char escapes itself by doubling). Emits one FlowFile.")
@WritesAttributes({
		@WritesAttribute(attribute = "parsecsv.record.count", description = "Number of records parsed (excluding header)"),
		@WritesAttribute(attribute = "parsecsv.header.present", description = "true if the first row was treated as header"),
		@WritesAttribute(attribute = "parsecsv.line.delimiter", description = "Effective line delimiter (after unescape)"),
		@WritesAttribute(attribute = "parsecsv.column.delimiter", description = "Effective column delimiter (after unescape)")
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
			.name("출력 파일을 라인 구분자")
			.description("출력 파일을 멀티 문자 기반 라인 구분자를 지정합니다. 멀티 캐릭터를 지원합니다. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("\\n")
			.build();

	public static final PropertyDescriptor OUTPUT_COLUMN_DELIMITER = new PropertyDescriptor.Builder()
			.name("출력 파일을 컬럼 구분자")
			.description("출력 파일의 멀티 문자 기반 컬럼 구분자를 지정합니다.. 이스케이프 지원: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue(",")
			.build();

	// ---- ETC ----
	public static final PropertyDescriptor COLUMN_COUNT = new PropertyDescriptor.Builder()
			.name("컬럼 카운트")
			.description("컬럼 카운트를 검증합니다.")
			.required(false)
			.defaultValue("0")
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.INTEGER_VALIDATOR)
			.build();

	public static final PropertyDescriptor INCLUDE_COLUMN_SEP_AT_LAST_COLUMN = new PropertyDescriptor.Builder()
			.name("마지막 컬럼에도 컬럼 구분자 존재 여부")
			.description("CSV의 경우 마지막 컬럼 뒤에 컬럼 구분자가 없으나 이 옵션을 체크하면 컬럼 구분자로 간주하여, '컬럼 카운트'에서 1개를 빼서 검증합니다.")
			.required(false)
			.allowableValues("true", "false")
			.defaultValue("false")
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
			.build();

	public static final PropertyDescriptor FIXED_SIZE_COLUMN = new PropertyDescriptor.Builder()
			.name("컬럼의 크기가 고정 크기시 컬럼의 문자수")
			.description("컬럼의 크기가 고정 크기로 되어 있는 컬럼의 경우 컬럼의 문자수를 검증합니다. 단 UTF로 가정하지 않고 Character의 개수를 검증하도록 합니다.")
			.required(false)
			.defaultValue("0")
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.INTEGER_VALIDATOR)
			.build();

	public static final PropertyDescriptor SKIP_EMPTY_LINE = new PropertyDescriptor.Builder()
			.name("빈 라인 건너뛰기")
			.description("파일을 로딩할 때 빈 라인은 처리하지 않고 SKIP합니다.")
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

		FlowFile ff = session.get();
		if (ff == null) return;

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

		final String lineDelim = unescape(lineDelimRaw);
		final String colDelim = unescape(colDelimRaw);
		final Character quoteChar = (quoteRaw == null || quoteRaw.isEmpty()) ? null : unescape(quoteRaw).charAt(0);
		final String outLineDelim = unescape(outLineRaw);
		final String outColDelim = unescape(outColRaw);


		if (lineDelim.isEmpty() || colDelim.isEmpty() || outLineDelim.isEmpty() || outColDelim.isEmpty()) {
			log.error("Delimiters must not be empty after unescape. inLine='{}' inCol='{}' outLine='{}' outCol='{}'",
					new Object[]{lineDelim, colDelim, outLineDelim, outColDelim});
			session.transfer(ff, REL_FAILURE);
			return;
		}

		final AtomicLong rowCount = new AtomicLong(0);
		FlowFile out = session.create(ff);

		CsvParserSettings settings = new CsvParserSettings();
		settings.setSkipEmptyLines(skipEmptyLine);                  // 빈 레코드도 유지 (필요 시 조절)
		settings.setHeaderExtractionEnabled(hasHeader);     // 헤더가 없다고 가정 (있다면 true)
		settings.setNullValue("");                          // null 대신 빈문자열
		settings.setEmptyValue("");                         // 빈 필드 값 보존

		CsvFormat format = settings.getFormat();
		if (quoteChar != null) {
			format.setQuote(quoteChar);                     // 기본 CSV 따옴표
			format.setQuoteEscape('"');                     // CSV 이스케이프 규칙 ("")
		}
		format.setLineSeparator(String.valueOf(RECORD_SEP));        // 레코드 구분자(단일문자)
		format.setDelimiter(COLUMN_SEP);                            // 컬럼 구분자(단일문자)

		try (final InputStream in = session.read(ff)) {
			out = session.write(out, outStream -> {
				try (OutputStreamWriter writer = new OutputStreamWriter(outStream, outCharset)) {
					try (Reader base = new BufferedReader(new InputStreamReader(in, inCharset), 8192);
					     Reader xform = new MultiDelimiterTranslatingReader(base, lineDelim, RECORD_SEP, colDelim, COLUMN_SEP)) {
						settings.setProcessor(new ReplaceRowProcessor(colDelim, outLineDelim, outColDelim, writer, rowCount, columnCount, includeColumnSepAtLastColumn, fixedSizeOfColumn));
						CsvParser parser = new CsvParser(settings);
						parser.parse(xform);
					}
				} catch (IOException e) {
					String msg = String.format("FlowFile을 읽어서 문자열을 Replace 처리하여 저장할 수 없습니다. 원인: %s", e.getMessage());
					log.warn(msg, e);
					throw new ProcessException(msg, e);
				}
			});

			final Map<String, String> attrs = new HashMap<>();
			attrs.put("parsecsv.record.count", String.valueOf(rowCount.get()));
			attrs.put("parsecsv.header.present", String.valueOf(hasHeader));
			attrs.put("parsecsv.line.delimiter", printable(lineDelim));
			attrs.put("parsecsv.column.delimiter", printable(colDelim));
			attrs.put("parsecsv.output.line.delimiter", printable(outLineDelim));
			attrs.put("parsecsv.output.column.delimiter", printable(outColDelim));
			attrs.put("mime.type", "text/plain; charset=" + outCharset.name());

			out = session.putAllAttributes(out, attrs);
			session.transfer(out, REL_SUCCESS);
			session.transfer(ff, REL_ORIGINAL);
		} catch (Exception e) {
			log.error("Failed to parse CSV: {}", e.getMessage(), e);
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

	private static String printable(String s) {
		return s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
	}

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
					.explanation(ok ? "OK" : "Must be exactly one character after unescape")
					.build();
		}
	}

}