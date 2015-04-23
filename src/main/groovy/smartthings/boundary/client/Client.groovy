package smartthings.boundary.client

import smartthings.boundary.domain.KafkaSerie

interface Client {

	void write(String topic, Integer partition, List<KafkaSerie> kafkaSeries)
}