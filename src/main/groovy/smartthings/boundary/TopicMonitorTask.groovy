package smartthings.boundary

import kafka.api.PartitionOffsetRequestInfo
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.javaapi.OffsetRequest
import kafka.javaapi.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFramework
import smartthings.boundary.util.ZkUtil

class TopicMonitorTask extends TimerTask {

	CuratorFramework curator
	List<String> topics
	String group

	Map<String, CachedConsumer> consumerMap = [:]

	TopicMonitorTask(CuratorFramework curator, List<String> topics, String group) {
		this.curator = curator
		this.topics = topics
		this.group = group
	}

	@Override
	void run() {
		for (String topic in topics) {
			processTopic(topic)
		}
	}

	void processTopic(String topic) {
		for (Integer partition in getPartitionsForTopic(topic)) {
			processPartition(topic, partition)
		}
	}

	@SuppressWarnings('Println')
	void processPartition(String topic, Integer partition) {
		Long offset = ZkUtil.getPartitionOffset(curator, group, topic, partition)
		Integer leader = ZkUtil.getLeaderForPartition(curator, topic, partition)

		if (leader) {
			SimpleConsumer consumer = getConsumer(topic, leader)

			if (consumer) {
				TopicAndPartition tap = new TopicAndPartition(topic, partition)
				PartitionOffsetRequestInfo partitionOffset = new PartitionOffsetRequestInfo(-1L, 1)

				Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = [:]
				requestInfo.put(tap, partitionOffset)

				OffsetRequest request = new OffsetRequest(requestInfo, (Short) 0, '')

				long logSize = consumer.getOffsetsBefore(request).offsets(topic, partition)[0]
				long lag = logSize - offset

				println("KAFKA_LOGSIZE ${topic}_${partition} ${logSize}")
				println("KAFKA_OFFSET ${topic}_${partition} ${offset}")
				println("KAFKA_LAG ${topic}_${partition} ${lag}")
			}
		}
	}

	List<Integer> getPartitionsForTopic(String topic) {
		return curator.children.forPath("/consumers/${group}/offsets/${topic}").collect { Integer.valueOf(it) }
	}

	SimpleConsumer getConsumer(String topic, Integer leader) {
		String key = "${topic}${leader}"

		SimpleConsumer consumer
		if (consumerMap.containsKey(key)) {
			CachedConsumer cachedConsumer = consumerMap.get(key)

			if (cachedConsumer.shouldRefresh()) {
				Broker broker = ZkUtil.getBroker(curator, leader)

				if (cachedConsumer.isStale(broker)) {
					cachedConsumer.consumer = ZkUtil.getConsumer(broker)
				}
			}

			consumer = cachedConsumer.consumer
		} else {
			consumer = ZkUtil.getConsumer(ZkUtil.getBroker(curator, leader))
			if (consumer) {
				consumerMap.put(key, new CachedConsumer(consumer: consumer))
			}
		}

		return consumer
	}
}
