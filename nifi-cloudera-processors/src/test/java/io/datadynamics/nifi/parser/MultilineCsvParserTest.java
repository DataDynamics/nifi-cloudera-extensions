package io.datadynamics.nifi.parser;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class MultilineCsvParserTest {

    private static TestRunner newRunner() {
        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        // 기본값: Column "^|" / Line "@@\n"
        runner.setProperty(MultilineCsvParser.INPUT_COLUMN_DELIMITER, "^|");
        runner.setProperty(MultilineCsvParser.INPUT_LINE_DELIMITER, "@@\\n");
        // 기본값: QUOTE="\"" / HAS_HEADER=false / charset=UTF-8
        return runner;
    }

    @Test
    void simple_noHeader_defaults_work_multiline() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.HAS_HEADER, "false");
        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "3");

        String input = "a^|b^|casdf\nasdfasdf@@\n";
        String expected = "a,b,casdf asdfasdf\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertTransferCount(MultilineCsvParser.REL_SUCCESS, 1);
        runner.assertTransferCount(MultilineCsvParser.REL_ORIGINAL, 1);
        runner.assertTransferCount(MultilineCsvParser.REL_FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "1");
        out.assertAttributeEquals("parsecsv.header.present", "false");
        out.assertAttributeEquals("parsecsv.input.line.delimiter", "@@\\n");
        out.assertAttributeEquals("parsecsv.input.column.delimiter", "^|");
        out.assertAttributeEquals("mime.type", "text/plain; charset=UTF-8");
    }

    @Test
    void simple_noHeader_defaults_work() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.HAS_HEADER, "false");
        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "3");

        String input = "a^|b^|c@@\n1^|2^|3@@\n";
        String expected = "a,b,c\n1,2,3\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertTransferCount(MultilineCsvParser.REL_SUCCESS, 1);
        runner.assertTransferCount(MultilineCsvParser.REL_ORIGINAL, 1);
        runner.assertTransferCount(MultilineCsvParser.REL_FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "2");
        out.assertAttributeEquals("parsecsv.header.present", "false");
        out.assertAttributeEquals("parsecsv.input.line.delimiter", "@@\\n");
        out.assertAttributeEquals("parsecsv.input.column.delimiter", "^|");
        out.assertAttributeEquals("mime.type", "text/plain; charset=UTF-8");
    }

    @Test
    void header_is_skipped_and_not_counted() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.HAS_HEADER, "true");
        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "2");

        String input = "H1^|H2@@\n1^|2@@\n3^|4@@\n";
        String expected = "1,2\n3,4\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "2");
        out.assertAttributeEquals("parsecsv.header.present", "true");
    }

    @Test
    void quotes_protect_delimiters_and_double_quote_is_unescaped() {
        TestRunner runner = newRunner();

	    runner.setProperty(MultilineCsvParser.QUOTE_CHAR, "\""); // quoting 해제
        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "3");

        // 기본 quote = '"'
        String input = "foo^|\"bar^|baz\"^|\"He said \"\"Hi\"\"\"@@\n";
        String expected = "foo,bar^|baz,He said \"Hi\"\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "1");
    }

    @Test
    void disable_quotes_treats_quote_as_literal() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.QUOTE_CHAR, "\""); // quoting 해제
        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "2");

        String input = "\"a\"^|\"b\"@@\n";
        String expected = "a,b\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "1");
    }

    @Test
    void eof_without_trailing_line_delimiter_writes_last_row() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "3");

        String input = "x^|y^|z"; // 마지막 @@\n 없음
        String expected = "x,y,z\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "1");
    }

    @Test
    void empty_fields_and_consecutive_column_delimiters() {
        TestRunner runner = newRunner();

        runner.setProperty(MultilineCsvParser.COLUMN_COUNT, "3");

        String input = "^|mid^|@@\n"; // "", "mid", ""  (3필드)
        String expected = ",mid,\n";

        runner.enqueue(input.getBytes(StandardCharsets.UTF_8));
        runner.run();

        MockFlowFile out = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        out.assertContentEquals(expected, StandardCharsets.UTF_8.name());
        out.assertAttributeEquals("parsecsv.record.count", "1");
    }
}