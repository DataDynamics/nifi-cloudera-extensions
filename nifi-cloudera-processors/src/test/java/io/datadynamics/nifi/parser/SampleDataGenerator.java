package io.datadynamics.nifi.parser;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;

public class SampleDataGenerator {

    public enum QuoteMode {NEVER, ALWAYS, AS_NEEDED}

    public static void main(String[] args) throws Exception {
        // 예) java SampleDataGenerator sample_input.dat 100 UTF-8 "^|" "@@\\n" "\"" AS_NEEDED true 0.08
        Path outPath = args.length > 0 ? Path.of(args[0]) : Path.of("input.dat");
        int rows = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        Charset charset = Charset.forName(args.length > 2 ? args[2] : "UTF-8");
        String colDelim = args.length > 3 ? args[3] : "^|";
        String rowDelim = args.length > 4 ? unescape(args[4]) : "@@\n";
        char quote = args.length > 5 ? args[5].charAt(0) : '"';
        QuoteMode quoteMode = args.length > 6 ? QuoteMode.valueOf(args[6]) : QuoteMode.AS_NEEDED;
        boolean withHeader = args.length > 7 ? Boolean.parseBoolean(args[7]) : true;
        double newlineProb = args.length > 8 ? Double.parseDouble(args[8]) : 0.08; // 가끔 \n 삽입 (기본 8%)

        generate(outPath, rows, charset, colDelim, rowDelim, quote, quoteMode, withHeader, newlineProb);
    }

    private static String unescape(String s) {
        return s.replace("\\r", "\r").replace("\\n", "\n").replace("\\t", "\t");
    }

    public static void generate(
            Path outPath,
            int rows,
            Charset charset,
            String colDelim,
            String rowDelim,
            char quote,
            QuoteMode quoteMode,
            boolean withHeader,
            double newlineProb
    ) throws IOException {
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(outPath), charset), 8192)) {
            if (withHeader) {
                writeRow(w, new String[]{
                        "id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email"
                }, colDelim, rowDelim, quote, quoteMode);
            }

            Random rnd = new Random(42);
            String[] cities = {"Seoul", "Busan", "Incheon", "Daegu", "Daejeon", "Gwangju", "Suwon", "Ulsan"};
            String[] status = {"ACTIVE", "INACTIVE", "PENDING"};

            for (int i = 1; i <= rows; i++) {
                String c0 = String.valueOf(i);
                String c1 = "사용자" + i;
                String c2 = cities[i % cities.length];
                String c3 = String.format(Locale.ROOT, "%.2f", rnd.nextDouble() * 100000.0);
                String c4 = String.format(Locale.ROOT, "2025-08-%02d", (i % 28) + 1);
                String c5 = status[i % status.length];

                String c6 = (i % 5 == 0) ? ("메모 " + colDelim + " 포함") : "일반 메모";
                String c7 = (i % 7 == 0) ? ("필드내 줄구분자 테스트") : "줄구분자 없음";
                String c8 = (i % 9 == 0) ? ("JSON: {\"k\":\"v" + i + "\"}") : "텍스트";
                String c9 = "user" + i + "@example.com";

                // 랜덤하게 '\n' 삽입 (중간에 넣음)
                c1 = injectNewline(c1, rnd, newlineProb);
                c6 = injectNewline(c6, rnd, newlineProb * 0.5);
                c7 = injectNewline(c7, rnd, newlineProb);
                c8 = injectNewline(c8, rnd, newlineProb * 0.3);

                writeRow(w, new String[]{c0, c1, c2, c3, c4, c5, c6, c7, c8, c9},
                        colDelim, rowDelim, quote, quoteMode);
            }
            w.flush();
        }
    }

    private static String injectNewline(String v, Random rnd, double p) {
        if (v == null || v.length() < 2) return v;
        if (rnd.nextDouble() < p) {
            int pos = 1 + rnd.nextInt(v.length() - 1); // 맨 앞/뒤는 피함
            return v.substring(0, pos) + "\n" + v.substring(pos);
        }
        return v;
    }

    private static void writeRow(BufferedWriter w, String[] cols,
                                 String colDelim, String rowDelim,
                                 char quote, QuoteMode mode) throws IOException {
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) w.write(colDelim);
            writeField(w, cols[i] == null ? "" : cols[i], colDelim, rowDelim, quote, mode);
        }
        w.write(rowDelim);
    }

    private static void writeField(BufferedWriter w, String v,
                                   String colDelim, String rowDelim,
                                   char quote, QuoteMode mode) throws IOException {
        boolean mustQuote;
        if (mode == QuoteMode.ALWAYS) {
            mustQuote = true;
        } else if (mode == QuoteMode.NEVER) {
            mustQuote = false;
        } else {
            mustQuote = v.indexOf(quote) >= 0 || v.contains(colDelim) || v.contains(rowDelim);
        }

        if (!mustQuote) {
            w.write(v);
            return;
        }

        String escaped = v.replace(String.valueOf(quote), String.valueOf(quote) + quote);
        w.write(quote);
        w.write(escaped);
        w.write(quote);
    }
}
