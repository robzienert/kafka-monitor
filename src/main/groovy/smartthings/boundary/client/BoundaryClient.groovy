package smartthings.boundary.client

import smartthings.boundary.domain.KafkaSerie

class BoundaryClient implements Client {

	@Override
	@SuppressWarnings('Println')
	void write(String topic, Integer partition, List<KafkaSerie> kafkaSeries) {
		for (KafkaSerie serie in kafkaSeries) {
			println("KAFKA_${serie.type.toString().toUpperCase()} ${topic}_${partition} ${serie.value}")
		}
	}
}
