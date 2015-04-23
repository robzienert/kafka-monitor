package smartthings.boundary.domain

enum SerieType {
	LOG_SIZE('logSize'),
	OFFSET('offset'),
	LAG('lag')

	private final String value

	SerieType(String value) {
		this.value = value
	}

	String toString() {
		return value
	}
}