package io.datadynamics.nifi.parser;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;

public class MultiDelimiterTranslatingReader extends Reader {
	private final PushbackReader in;
	private final String recDelim;
	private final char recRep;
	private final String colDelim;
	private final char colRep;

	public MultiDelimiterTranslatingReader(Reader src, String recordDelimiter, char recordReplacement, String columnDelimiter, char columnReplacement) {
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
