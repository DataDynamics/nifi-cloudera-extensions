package io.datadynamics.nifi.parser;


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

        final long[] recordCount = new long[]{0};
        FlowFile out = session.create(input);

        try (final InputStream in = session.read(input)) {
            out = session.write(out, outStream -> {
                try (OutputStreamWriter writer = new OutputStreamWriter(outStream, outCharset)) {
                    MultiDelimCsvStreamer streamer =
                            new MultiDelimCsvStreamer(lineDelim, colDelim, quoteChar, inCharset, outLineDelim, outColDelim);
                    streamer.stream(in, hasHeader, writer, recordCount);
                    writer.flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            final Map<String, String> attrs = new HashMap<>();
            attrs.put("parsecsv.record.count", String.valueOf(recordCount[0]));
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

    // ---- Streaming Parser/Writer ----
    private static class MultiDelimCsvStreamer {
        private final String lineDelim;
        private final String colDelim;
        private final char[] lineArr;
        private final char[] colArr;
        private final int maxDelimLen;
        private final Character quoteChar;
        private final Charset inputCharset;

        private final String outLineDelim;
        private final String outColDelim;

        MultiDelimCsvStreamer(String lineDelim, String colDelim, Character quoteChar, Charset inputCharset,
                              String outLineDelim, String outColDelim) {
            this.lineDelim = Objects.requireNonNull(lineDelim);
            this.colDelim = Objects.requireNonNull(colDelim);
            this.lineArr = lineDelim.toCharArray();
            this.colArr = colDelim.toCharArray();
            this.maxDelimLen = Math.max(Math.max(lineArr.length, colArr.length), 3); // >=3 for BOM look
            this.quoteChar = quoteChar;
            this.inputCharset = inputCharset;
            this.outLineDelim = Objects.requireNonNull(outLineDelim);
            this.outColDelim = Objects.requireNonNull(outColDelim);
        }

        void stream(InputStream in, boolean hasHeader, Writer out, long[] rowCounter) throws IOException {
            try (BufferedReader base = new BufferedReader(new InputStreamReader(in, inputCharset), 8192)) {
                PushbackReader reader = new PushbackReader(base, Math.max(maxDelimLen, 1));
                reader = skipBom(reader);

                boolean inQuotes = false;
                boolean skippingHeader = hasHeader;  // 첫 행 출력 생략
                boolean rowStarted = false;          // 현재 데이터 행에 뭔가라도 썼는지

                while (true) {
                    int ich = reader.read();
                    if (ich == -1) {
                        if (!skippingHeader && rowStarted) {
                            out.write(outLineDelim);
                            rowCounter[0]++;
                        }
                        break;
                    }
                    char c = (char) ich;

                    // ---- Quote handling ----
                    if (quoteChar != null) {
                        if (inQuotes) {
                            if (c == quoteChar) {
                                int next = reader.read();
                                if (next == -1) {
                                    inQuotes = false;
                                } else {
                                    char nc = (char) next;
                                    if (nc == quoteChar) {
                                        if (!skippingHeader) {
                                            out.write(quoteChar);
                                            rowStarted = true;
                                        }
                                    } else {
                                        inQuotes = false;
                                        reader.unread(nc);
                                    }
                                }
                            } else {
                                if (!skippingHeader) {
                                    out.write(c == '\n' ? ' ' : c); // \n → 컬럼안에 \n인 경우 Space로 변경
                                    rowStarted = true;
                                }
                            }
                            continue;
                        } else if (c == quoteChar) {
                            inQuotes = true;
                            continue;
                        }
                    }

                    // ---- Delimiters ----
                    Match m = matchesAhead(reader, c, lineArr, colArr);
                    if (m == Match.LINE) {
                        if (skippingHeader) {
                            skippingHeader = false;
                        } else {
                            out.write(outLineDelim);
                            rowCounter[0]++;
                        }
                        rowStarted = false;
                        continue;
                    } else if (m == Match.COLUMN) {
                        if (!skippingHeader) {
                            out.write(outColDelim);
                            rowStarted = true; // empty field allowed
                        }
                        continue;
                    }

                    // ---- Regular char ----
                    if (!skippingHeader) {
                        out.write(c == '\n' ? ' ' : c); // \n → 컬럼안에 \n인 경우 Space로 변경
                        rowStarted = true;
                    }
                }
            }
        }

        private enum Match {NONE, LINE, COLUMN}

        /**
         * Prefer full matches; tie -> prefer 'primary'. At EOF, accept a prefix of the LINE delimiter as a line break.
         */
        private Match matchesAhead(PushbackReader reader, char firstChar, char[] primary, char[] secondary) throws IOException {
            boolean couldPrimary = firstChar == primary[0];
            boolean couldSecondary = firstChar == secondary[0];
            if (!couldPrimary && !couldSecondary) return Match.NONE;

            char[] buf = new char[Math.max(maxDelimLen - 1, 0)];
            int read = reader.read(buf, 0, buf.length);
            if (read < 0) read = 0;

            int windowLen = 1 + read;
            boolean primaryFull = startsWithFull(firstChar, buf, windowLen, primary);
            boolean secondaryFull = startsWithFull(firstChar, buf, windowLen, secondary);

            Match result = Match.NONE;
            int consumeLen = 0;

            if (primaryFull && secondaryFull) {
                if (primary.length > secondary.length) {
                    result = (primary == lineArr) ? Match.LINE : Match.COLUMN;
                    consumeLen = primary.length;
                } else if (secondary.length > primary.length) {
                    result = (secondary == lineArr) ? Match.LINE : Match.COLUMN;
                    consumeLen = secondary.length;
                } else {
                    result = (primary == lineArr) ? Match.LINE : Match.COLUMN;
                    consumeLen = primary.length;
                }
            } else if (primaryFull) {
                result = (primary == lineArr) ? Match.LINE : Match.COLUMN;
                consumeLen = primary.length;
            } else if (secondaryFull) {
                result = (secondary == lineArr) ? Match.LINE : Match.COLUMN;
                consumeLen = secondary.length;
            } else {
                // EOF에서 line delimiter의 접두만 남은 경우 허용
                boolean atStreamEnd = read < buf.length;
                boolean primaryPrefix = startsWithPrefix(firstChar, buf, windowLen, primary);
                if (atStreamEnd && primary == lineArr && primaryPrefix) {
                    result = Match.LINE;
                    consumeLen = windowLen;
                }
            }

            int extra = read - Math.max(0, consumeLen - 1);
            if (result != Match.NONE) {
                if (extra > 0) {
                    reader.unread(buf, consumeLen - 1, extra);
                } else if (consumeLen > windowLen) {
                    if (read > 0) reader.unread(buf, 0, read);
                    return Match.NONE;
                }
            } else {
                if (read > 0) reader.unread(buf, 0, read);
            }
            return result;
        }

        private boolean startsWithFull(char first, char[] buf, int windowLen, char[] delim) {
            if (windowLen < delim.length) return false;
            if (first != delim[0]) return false;
            for (int i = 1; i < delim.length; i++) {
                if (buf[i - 1] != delim[i]) return false;
            }
            return true;
        }

        private boolean startsWithPrefix(char first, char[] buf, int windowLen, char[] delim) {
            if (first != delim[0]) return false;
            int upto = Math.min(windowLen, delim.length);
            for (int i = 1; i < upto; i++) {
                if (buf[i - 1] != delim[i]) return false;
            }
            return true;
        }

        // mark/reset 없이 BOM 처리 (UTF-8 BOM은 '\uFEFF'로 디코드됨)
        private PushbackReader skipBom(PushbackReader r) throws IOException {
            int c = r.read();
            if (c != -1 && c != '\uFEFF') {
                r.unread(c);
            }
            return r;
        }
    }
}