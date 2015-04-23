package smartthings.boundary.domain

import groovy.transform.Immutable

@Immutable
class KafkaSerie {
	SerieType type
	Long value
}
