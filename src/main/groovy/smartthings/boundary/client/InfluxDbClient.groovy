package smartthings.boundary.client

import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Serie
import smartthings.boundary.domain.KafkaSerie

import java.util.concurrent.TimeUnit

class InfluxDbClient implements Client {

	private final InfluxDB influxDb
	private final String dbName

	InfluxDbClient(String connect, String dbName, String username, String password) {
		influxDb = InfluxDBFactory.connect(connect, username, password)
		this.dbName = dbName
	}

	@Override
	void write(String topic, Integer partition, List<KafkaSerie> kafkaSeries) {
		long now = System.currentTimeMillis() / 1000
		List<Serie> series = kafkaSeries.collect { KafkaSerie ks ->
			Serie serie = new Serie.Builder("kafka.${topic}.${partition}.${ks.type}")
					.columns('time', 'value')
					.values(now, ks.value)
					.build()
			return serie
		}

		influxDb.write(dbName, TimeUnit.SECONDS, *series)
	}
}
