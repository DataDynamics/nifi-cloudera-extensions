package io.datadynamics.nifi.parser;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class PrettyHexDump {

	private static final int BYTES_PER_LINE = 16;

	/**
	 * 편의용: UTF-8로 문자열을 예쁘게 헥스 덤프
	 */
	public static String prettyHexDump(String text) {
		return prettyHexDump(text, StandardCharsets.UTF_8);
	}

	/**
	 * 지정한 문자셋으로 문자열 → 바이트 후 예쁘게 헥스 덤프
	 */
	public static String prettyHexDump(String text, Charset charset) {
		return prettyHexDump(text.getBytes(charset));
	}

	/**
	 * 바이트 배열 전체를 예쁘게 헥스 덤프 (베이스 오프셋 0)
	 */
	public static String prettyHexDump(byte[] data) {
		return prettyHexDump(data, 0, data.length, 0);
	}

	/**
	 * 바이트 배열의 일부를 예쁘게 헥스 덤프
	 *
	 * @param data              대상 바이트
	 * @param offset            시작 오프셋
	 * @param length            길이
	 * @param displayBaseOffset 출력에 표시할 베이스 오프셋(예: 0x1000부터 시작해서 보이고 싶을 때)
	 */
	public static String prettyHexDump(byte[] data, int offset, int length, int displayBaseOffset) {
		if (offset < 0 || length < 0 || offset + length > data.length)
			throw new IndexOutOfBoundsException("Invalid offset/length");

		StringBuilder sb = new StringBuilder(length * 4 + 64);

		for (int i = 0; i < length; i += BYTES_PER_LINE) {
			int lineLen = Math.min(BYTES_PER_LINE, length - i);

			// 왼쪽 오프셋 (8자리 대문자 HEX)
			sb.append(String.format("%08X  ", displayBaseOffset + i));

			// HEX 구역 (8바이트마다 한 칸 추가)
			for (int j = 0; j < BYTES_PER_LINE; j++) {
				if (j == 8) sb.append(' ');
				if (j < lineLen) {
					sb.append(String.format("%02X ", data[offset + i + j] & 0xFF));
				} else {
					sb.append("   "); // 패딩
				}
			}

			// ASCII 구역
			sb.append(" ");
			for (int j = 0; j < lineLen; j++) {
				int b = data[offset + i + j] & 0xFF;
				sb.append((b >= 0x20 && b <= 0x7E) ? (char) b : '.');
			}
			// 마지막 라인 패딩
			for (int j = lineLen; j < BYTES_PER_LINE; j++) sb.append(' ');
			sb.append("\n");
		}
		return sb.toString();
	}

	// 사용 예시
	public static void main(String[] args) {
		String s = "Hello, Netty!\n안녕하세요 Line2";
		System.out.print(PrettyHexDump.prettyHexDump(s));
	}
}