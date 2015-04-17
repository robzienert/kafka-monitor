package smartthings.boundary

import kafka.cluster.Broker
import kafka.javaapi.consumer.SimpleConsumer
import org.joda.time.DateTime

import java.util.concurrent.TimeUnit

class CachedConsumer {

	final static long REFRESH_INTERVAL = TimeUnit.SECONDS.toMillis(10)

	SimpleConsumer consumer

	long cached = new DateTime().millis

	boolean shouldRefresh() {
		long now = new DateTime().millis
		boolean refresh = now - cached > REFRESH_INTERVAL
		if (refresh) {
			cached = now
		}
		return refresh
	}

	boolean isStale(Broker newBroker) {
		return !"${consumer.host()}${consumer.port()}".equals("${newBroker.host()}${newBroker.port()}")
	}
}