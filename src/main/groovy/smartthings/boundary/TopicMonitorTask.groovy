package smartthings.boundary

import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi.OffsetRequest
import kafka.javaapi.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFramework
import smartthings.boundary.util.ZkUtil

class TopicMonitorTask extends TimerTask {

	CuratorFramework curator
	List<String> topics
	String group

	Map<Integer, SimpleConsumer> consumerMap = [:]

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
		String offsetData = curator.data.forPath("/consumers/${group}/offsets/${topic}/${partition}")?.toString()
		Long offset = offsetData == null ? 0L : Long.valueOf(offsetData)

		Integer leader = ZkUtil.getLeaderForPartition(curator, topic, partition)
		if (leader) {
			SimpleConsumer consumer = getConsumer(leader)

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
		return curator.children.forPath("/consumers/${group}/offsets/${topic}").collect { (Integer) it }
	}

	SimpleConsumer getConsumer(Integer leader) {
		// TODO Need to support re-connecting to newly elected leaders.
		if (!consumerMap.containsKey(leader)) {
			consumerMap.put(leader, ZkUtil.getConsumer(curator, leader))
		}
		return consumerMap.get(leader)
	}
}
