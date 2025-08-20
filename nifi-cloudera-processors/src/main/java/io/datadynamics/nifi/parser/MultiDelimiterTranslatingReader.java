package io.datadynamics.nifi.parser;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;

/**
 * 읽기 작업 중에 지정된 레코드 및 컬럼 구분자를 지정된 대체 문자로 치환하는 Reader 구현입니다.
 * 이 클래스는 실시간으로 구분자를 대체하여 입력 데이터 스트림을 투명하게 변환할 수 있는 방법을 제공합니다.
 */
public class MultiDelimiterTranslatingReader extends Reader {

	/**
	 * CSV Parser가 2바이트만 지원하여 동작시 문제가 없도록 처리하기 위한 NiFi Processor에서 기본 Line Delimiter.
	 */
	private final char recRep;

	/**
	 * CSV Parser가 2바이트만 지원하여 동작시 문제가 없도록 처리하기 위한 NiFi Processor에서 기본 Column Delimiter.
	 */
	private final char colRep;

	/**
	 * Line Delimiter
	 */
	private final String recDelim;

	/**
	 * Column Delimiter
	 */
	private final String colDelim;

	private final PushbackReader in;

	/**
	 * 제공된 Reader를 감싸며, 읽기 작업 동안 지정된 구분자를 지정된 대체 문자로 치환하는
	 * MultiDelimiterTranslatingReader 인스턴스를 생성합니다.
	 *
	 * @param src               래핑할 소스 Reader
	 * @param recordDelimiter   대체 대상인 레코드 구분자를 나타내는 문자열
	 * @param recordReplacement 레코드 구분자 발생 시 사용할 대체 문자
	 * @param columnDelimiter   대체 대상인 컬럼 구분자를 나타내는 문자열
	 * @param columnReplacement 컬럼 구분자 발생 시 사용할 대체 문자
	 */
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

	/**
	 * 입력 스트림에서 다음 문자를 읽습니다. 문자가 지정된 구분자(레코드 또는 컬럼)의 시작과
	 * 일치하면 해당 구분자를 지정된 대체 문자로 치환하려 시도합니다. 일치하지 않으면
	 * 원래 문자를 변경 없이 반환합니다.
	 *
	 * @return 스트림의 다음 문자, 구분자가 일치한 경우 대체 문자, 또는 스트림의 끝에 도달한 경우 -1
	 * @throws IOException 읽기 작업 중 I/O 오류가 발생한 경우
	 */
	@Override
	public int read() throws IOException {
		int c = in.read();
		if (c == '\u001F') { // TODO 문자열내에 특수 문자가 포함되면 Line Delimiter로 인식할 수 있다.
			return -1;
		}
		if (c == -1) return -1;

		// 두 종류의 구분자를 우선순위대로 검사 (레코드 → 컬럼)
		int replaced = tryMatchAndReplace(c, recDelim, recRep);
		if (replaced != Integer.MIN_VALUE) return replaced;

		replaced = tryMatchAndReplace(c, colDelim, colRep);
		if (replaced != Integer.MIN_VALUE) return replaced;

		return c;
	}

	/**
	 * 첫 번째 문자와 그 이후 문자들이 지정된 구분자와 일치하는지 확인을 시도합니다.
	 * 일치하면 해당 구분자를 지정된 대체 문자로 치환합니다.
	 * 일치하지 않으면 읽은 문자를 모두 원상 복구하고, 메서드는 실패를 나타내는 값을 반환합니다.
	 *
	 * @param firstChar   구분자의 시작과 비교할 첫 번째 문자
	 * @param delimiter   일치 여부를 확인할 구분자 문자열
	 * @param replacement 일치 시 구분자를 대체할 문자
	 * @return 일치한 경우 대체 문자; 그렇지 않으면 실패를 나타내기 위해 Integer.MIN_VALUE를 반환
	 * @throws IOException 문자를 읽거나 되돌리는 과정에서 I/O 오류가 발생한 경우
	 */
	private int tryMatchAndReplace(int firstChar, String delimiter, char replacement) throws IOException {
		if (delimiter == null || delimiter.isEmpty()) return Integer.MIN_VALUE;
		if (firstChar != delimiter.charAt(0)) return Integer.MIN_VALUE;

		int len = delimiter.length();
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
			if (buf[i - 1] != delimiter.charAt(i)) {
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

	/**
	 * 문자 배열의 일부에 문자를 읽어들입니다. 이 메서드는 지정된 오프셋부터 제공된 버퍼에 최대 지정된 개수만큼의 문자를 읽어들이려고 시도합니다.
	 *
	 * @param cbuf 문자를 읽어들일 문자 배열
	 * @param off  문자를 저장하기 시작할 배열 내 인덱스
	 * @param len  읽을 문자의 최대 개수
	 * @return 실제로 읽은 문자 수, 또는 스트림의 끝에 도달한 경우 -1
	 * @throws IOException I/O 오류가 발생한 경우
	 */
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

	/**
	 * PushbackReader를 종료합니다.
	 *
	 * @throws IOException 종료할 수 없는 경우
	 */
	@Override
	public void close() throws IOException {
		in.close();
	}
}
