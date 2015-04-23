package smartthings.kafkamonitor.domain

import groovy.transform.Immutable

@Immutable
class KafkaSerie {
	SerieType type
	Long value
}
