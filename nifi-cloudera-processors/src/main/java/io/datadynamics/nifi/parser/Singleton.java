package io.datadynamics.nifi.parser;

public final class Singleton<T> {

	private static Singleton<?> instance;

	private final T value;

	private Singleton(T value) {
		this.value = value;
	}

	/**
	 * 최초 1회만 인스턴스를 주입
	 */
	public static synchronized <T> void init(T value) {
		if (instance != null) {
			throw new IllegalStateException("Already initialized");
		}
		instance = new Singleton<>(value);
	}

	/**
	 * 주입된 인스턴스 반환
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getInstance() {
		if (instance == null) {
			throw new IllegalStateException("Not initialized yet");
		}
		return (T) instance.value;
	}
}