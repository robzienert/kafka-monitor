package smartthings.kafkamonitor.util

import groovy.json.JsonSlurper
import kafka.cluster.Broker
import kafka.javaapi.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFramework

class ZkUtil {

	static Integer getLeaderForPartition(CuratorFramework curator, String topic, Integer partition) {
		byte[] stateData = curator.data.forPath("/brokers/topics/${topic}/partitions/${partition}/state")
		if (stateData) {
			Map<String, Object> data = (Map<String, Object>) new JsonSlurper().parse(stateData)
			return (Integer) data?.leader
		}
		return null
	}

	static Broker getBroker(CuratorFramework curator, Integer brokerId) {
		byte[] brokerData = curator.data.forPath("/brokers/ids/${brokerId}")
		if (brokerData) {
			Map<String, Object> data = (Map<String, Object>) new JsonSlurper().parse(brokerData)
			return new Broker(brokerId, (String) data.host, (Integer) data.port)
		}
		return null
	}

	static Long getPartitionOffset(CuratorFramework curator, String group, String topic, Integer partition) {
		Long offset = 0L

		byte[] offsetData = curator.data.forPath("/consumers/${group}/offsets/${topic}/${partition}")
		if (offsetData) {
			ByteArrayOutputStream baos
			try {
				baos = new ByteArrayOutputStream()
				baos.write(offsetData)
				offset = Long.valueOf(baos.toString())
			} finally {
				if (baos) {
					baos.close()
				}
			}

			return offset
		}

		return offset
	}

	static SimpleConsumer getConsumer(Broker broker) {
		return new SimpleConsumer(
				broker.host(),
				broker.port(),
				10000,
				100000,
				'ConsumerOffsetMonitor'
		)
	}
}
