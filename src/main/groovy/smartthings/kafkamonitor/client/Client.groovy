package smartthings.kafkamonitor.client

import smartthings.kafkamonitor.domain.KafkaSerie

interface Client {

	void write(String topic, Integer partition, List<KafkaSerie> kafkaSeries)
}