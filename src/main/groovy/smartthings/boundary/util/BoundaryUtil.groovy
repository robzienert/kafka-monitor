package smartthings.boundary.util

class BoundaryUtil {

	static String metricName(String metric, String topic, Integer partition) {
		return "KAFKA_${metric.toUpperCase()}_${topic.toUpperCase()}_${partition}"
	}
}
