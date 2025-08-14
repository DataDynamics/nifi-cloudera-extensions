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

	private static final char RECORD_SEP = '\u001E'; // RS (Record Separator)
	private static final char COLUMN_SEP = '\u001F'; // US (Unit Separator)

	// ---- Input Delimiters ----
	public static final PropertyDescriptor LINE_DELIMITER = new PropertyDescriptor.Builder()
			.name("Line Delimiter")
			.description("Input line delimiter (multi-character). Supports escapes: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("@@\\n")
			.build();

	public static final PropertyDescriptor COLUMN_DELIMITER = new PropertyDescriptor.Builder()
			.name("Column Delimiter")
			.description("Input column delimiter (multi-character). Supports escapes: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("^|")
			.build();

	// ---- Quoting & Header ----
	public static final PropertyDescriptor QUOTE_CHAR = new PropertyDescriptor.Builder()
			.name("Quote Character")
			.description("Optional quote character for fields. Leave empty for no quoting. " +
					"If set (e.g. '\"'), inside quoted fields delimiters are ignored and the quote is escaped by doubling (\"\").")
			.required(false)
			.addValidator(new SingleCharAfterUnescapeValidator())
			.defaultValue("\"")
			.build();

	public static final PropertyDescriptor HAS_HEADER = new PropertyDescriptor.Builder()
			.name("Has Header Row")
			.description("Treat the first row as header and DO NOT output it.")
			.required(false)
			.allowableValues("true", "false")
			.defaultValue("false")
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
			.build();

	// ---- Charsets ----
	public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
			.name("Input Character Set")
			.description("Charset for decoding input bytes. Example: CP949 (a.k.a. MS949).")
			.required(true)
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
			.defaultValue("CP949") // 기본: CP949
			.build();

	public static final PropertyDescriptor OUTPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
			.name("Output Character Set")
			.description("Charset for encoding output text. Example: UTF-8.")
			.required(true)
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
			.defaultValue(StandardCharsets.UTF_8.name()) // 기본: UTF-8
			.build();

	// ---- Output Delimiters ----
	public static final PropertyDescriptor OUTPUT_LINE_DELIMITER = new PropertyDescriptor.Builder()
			.name("Output Line Delimiter")
			.description("Output line delimiter (multi-character). Supports escapes: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("\\n")
			.build();

	public static final PropertyDescriptor OUTPUT_COLUMN_DELIMITER = new PropertyDescriptor.Builder()
			.name("Output Column Delimiter")
			.description("Output column delimiter (multi-character). Supports escapes: \\n, \\r, \\t, \\\\, \\uXXXX.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue(",")
			.build();

	// ---- Relationships ----
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Plain text output with configured output delimiters/charset.")
			.build();

	public static final Relationship REL_ORIGINAL = new Relationship.Builder()
			.name("original")
			.description("Original input FlowFile.")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("If parsing fails, the original FlowFile goes here.")
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

		FlowFile input = session.get();
		if (input == null) return;

		final String lineDelimRaw = context.getProperty(LINE_DELIMITER).getValue();
		final String colDelimRaw = context.getProperty(COLUMN_DELIMITER).getValue();
		final String quoteRaw = context.getProperty(QUOTE_CHAR).getValue();
		final boolean hasHeader = context.getProperty(HAS_HEADER).asBoolean();

		final Charset inCharset = Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
		final Charset outCharset = Charset.forName(context.getProperty(OUTPUT_CHARACTER_SET).getValue());

		final String outLineRaw = context.getProperty(OUTPUT_LINE_DELIMITER).getValue();
		final String outColRaw = context.getProperty(OUTPUT_COLUMN_DELIMITER).getValue();

		final String lineDelim = unescape(lineDelimRaw);
		final String colDelim = unescape(colDelimRaw);
		final Character quoteChar = (quoteRaw == null || quoteRaw.isEmpty()) ? null : unescape(quoteRaw).charAt(0);
		final String outLineDelim = unescape(outLineRaw);
		final String outColDelim = unescape(outColRaw);


		if (lineDelim.isEmpty() || colDelim.isEmpty() || outLineDelim.isEmpty() || outColDelim.isEmpty()) {
			log.error("Delimiters must not be empty after unescape. inLine='{}' inCol='{}' outLine='{}' outCol='{}'",
					new Object[]{lineDelim, colDelim, outLineDelim, outColDelim});
			session.transfer(input, REL_FAILURE);
			return;
		}

		final AtomicLong count = new AtomicLong(0);
		FlowFile out = session.create(input);

		CsvParserSettings settings = new CsvParserSettings();
		settings.setSkipEmptyLines(false);          // 빈 레코드도 유지 (필요 시 조절)
		settings.setHeaderExtractionEnabled(false); // 헤더가 없다고 가정 (있다면 true)
		settings.setNullValue("");                  // null 대신 빈문자열
		settings.setEmptyValue("");                 // 빈 필드 값 보존

		CsvFormat format = settings.getFormat();
		if (quoteChar != null) {
			format.setQuote(quoteChar);             // 기본 CSV 따옴표
			format.setQuoteEscape('"');             // CSV 이스케이프 규칙 ("")
		}
		format.setLineSeparator(String.valueOf(RECORD_SEP));         // 레코드 구분자(단일문자)
		format.setDelimiter(COLUMN_SEP);              // 컬럼 구분자(단일문자)

		String refinedLineDelimiter = lineDelim.replace("\r\n", "").replace("\n", "");

		try (final InputStream in = session.read(input)) {
			out = session.write(out, outStream -> {
				try (OutputStreamWriter writer = new OutputStreamWriter(outStream, outCharset)) {
					try (Reader base = new BufferedReader(new InputStreamReader(in, inCharset), 8192);
					     Reader xform = new MultiDelimiterTranslatingReader(base, lineDelim, RECORD_SEP, colDelim, COLUMN_SEP)) {
						settings.setProcessor(new ReplaceRowProcessor(outLineDelim, outColDelim, writer, refinedLineDelimiter, count));
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
			attrs.put("parsecsv.record.count", String.valueOf(count.get()));
			attrs.put("parsecsv.header.present", String.valueOf(hasHeader));
			attrs.put("parsecsv.line.delimiter", printable(lineDelim));
			attrs.put("parsecsv.column.delimiter", printable(colDelim));
			attrs.put("parsecsv.output.line.delimiter", printable(outLineDelim));
			attrs.put("parsecsv.output.column.delimiter", printable(outColDelim));
			attrs.put("mime.type", "text/plain; charset=" + outCharset.name());

			out = session.putAllAttributes(out, attrs);
			session.transfer(out, REL_SUCCESS);
			session.transfer(input, REL_ORIGINAL);
		} catch (Exception e) {
			log.error("Failed to parse CSV: {}", e.getMessage(), e);
			session.remove(out);
			input = session.putAttribute(input, "parsecsv.error", String.valueOf(e));
			session.transfer(input, REL_FAILURE);
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