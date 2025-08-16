package io.datadynamics.nifi.parser;

import shaded.com.univocity.parsers.common.ParsingContext;
import shaded.com.univocity.parsers.common.processor.AbstractRowProcessor;
import shaded.com.univocity.parsers.csv.CsvFormat;
import shaded.com.univocity.parsers.csv.CsvParser;
import shaded.com.univocity.parsers.csv.CsvParserSettings;

import java.io.*;
import java.nio.charset.Charset;

public class UnivocityCustomParserExample {

    // 제어문자: 실제 데이터에 나타나지 않을 값을 쓰는 것을 권장
    private static final char RECORD_SEP = '\u001E'; // RS (Record Separator)
    private static final char COLUMN_SEP = '\u001F'; // US (Unit Separator)

    public static void main(String[] args) throws Exception {
        File input = new File("input.txt");          // 원본 파일
        Charset cs = Charset.forName("UTF-8");       // 입력 문자셋 (예: UTF-8/CP949)

        // 다문자 레코드/컬럼 구분자
        String multiRecordSep = "@@\r\n";
        String multiColumnSep = "^|";
        String cleanMultiRecordSep = multiRecordSep.replace("\r\n", "").replace("\n", "");

        // 1) 입력 스트림을 커스텀 Reader로 감싸서 다문자 구분자를 단일 문자로 변환
        try (Reader base = new BufferedReader(new InputStreamReader(new FileInputStream(input), cs), 8192);
             Reader xform = new MultiDelimiterTranslatingReader(base, multiRecordSep, RECORD_SEP, multiColumnSep, COLUMN_SEP)) {

            // 2) uniVocity 설정
            CsvParserSettings settings = new CsvParserSettings();
            settings.setSkipEmptyLines(false);   // 빈 레코드도 유지 (필요 시 조절)
            settings.setHeaderExtractionEnabled(false); // 헤더가 없다고 가정 (있다면 true)
            settings.setNullValue("");           // null 대신 빈문자열
            settings.setEmptyValue("");          // 빈 필드 값 보존

            CsvFormat format = settings.getFormat();
            format.setQuote('"');                // 기본 CSV 따옴표
            format.setQuoteEscape('"');          // CSV 이스케이프 규칙 ("")
            format.setLineSeparator(String.valueOf(RECORD_SEP)); // 레코드 구분자(단일문자)
            format.setDelimiter(COLUMN_SEP);     // 컬럼 구분자(단일문자)

            // 3) RowProcessor(콜백) 등록: 레코드가 한 건씩 들어올 때마다 처리
            final String[] radsasdf = {null};
            settings.setProcessor(new AbstractRowProcessor() {
                @Override
                public void rowProcessed(String[] rows, ParsingContext context) {
                    long n = context.currentRecord(); // 1부터 시작
                    for (String row : rows) {
                        System.out.println(row.replace("\r\n", " ").replace("\n", " "));
                    }

                    if (rows[rows.length - 1].endsWith(cleanMultiRecordSep)) {
                        radsasdf[0] = "asdf";
                    }
                }
            });

            // 4) 파싱 시작 (스트리밍)
            CsvParser parser = new CsvParser(settings);
            parser.parse(xform);
        }
    }

    /**
     * PushbackReader를 이용해 다문자 구분자를 단일 문자로 바꾸는 Reader.
     * - 인용부호 안/밖 구분까지는 하지 않지만, CSV 파서가 따옴표 안의 구분문자는 무시하므로 일반적으로 안전합니다.
     * - 필요 시 따옴표 인식 로직을 추가해 '따옴표 밖에서만 치환'하도록 확장 가능합니다.
     */
    public static class MultiDelimiterTranslatingReader extends Reader {
        private final PushbackReader in;
        private final String recDelim;
        private final char recRep;
        private final String colDelim;
        private final char colRep;

        public MultiDelimiterTranslatingReader(Reader src,
                                               String recordDelimiter, char recordReplacement,
                                               String columnDelimiter, char columnReplacement) {
            super(src);
            this.recDelim = recordDelimiter;
            this.recRep = recordReplacement;
            this.colDelim = columnDelimiter;
            this.colRep = columnReplacement;

            // 가장 긴 구분자 길이만큼 lookahead가 필요
            int maxLen = Math.max(recDelim != null ? recDelim.length() : 0, colDelim != null ? colDelim.length() : 0);
            if (maxLen < 2) maxLen = 2; // 최소 2
            this.in = new PushbackReader(src, maxLen);
        }

        @Override
        public int read() throws IOException {
            int c = in.read();
            if (c == -1) return -1;

            // 두 종류의 구분자를 우선순위대로 검사 (레코드 → 컬럼)
            int replaced = tryMatchAndReplace(c, recDelim, recRep);
            if (replaced != Integer.MIN_VALUE) return replaced;

            replaced = tryMatchAndReplace(c, colDelim, colRep);
            if (replaced != Integer.MIN_VALUE) return replaced;

            return c;
        }

        private int tryMatchAndReplace(int firstChar, String delim, char replacement) throws IOException {
            if (delim == null || delim.isEmpty()) return Integer.MIN_VALUE;
            if (firstChar != delim.charAt(0)) return Integer.MIN_VALUE;

            int len = delim.length();
            if (len == 1) {
                // 이론상 올 일 없지만 방어
                return replacement;
            }

            char[] buf = new char[len - 1];
            int actuallyRead = in.read(buf, 0, len - 1);
            if (actuallyRead != len - 1) {
                // 더 이상 읽을 수 없으면 되돌리고 실패
                if (actuallyRead > 0) in.unread(buf, 0, actuallyRead);
                return Integer.MIN_VALUE;
            }

            // firstChar + buf 가 delim 과 같은지 확인
            // firstChar는 int 이므로 캐스팅 주의
            boolean match = true;
            for (int i = 1; i < len; i++) {
                if (buf[i - 1] != delim.charAt(i)) {
                    match = false;
                    break;
                }
            }

            if (match) {
                // 매치 성공: delim 전체를 replacement 1글자로 대체
                return replacement;
            } else {
                // 매치 실패: 읽은 것들 되돌리고 실패 표시
                in.unread(buf, 0, len - 1);
                return Integer.MIN_VALUE;
            }
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (len <= 0) return 0;
            int i = 0;
            for (; i < len; i++) {
                int c = read();
                if (c == -1) break;
                cbuf[off + i] = (char) c;
            }
            return (i == 0) ? -1 : i;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
