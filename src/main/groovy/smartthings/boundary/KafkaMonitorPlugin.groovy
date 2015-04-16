package smartthings.boundary

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

@SuppressWarnings('DuplicateNumberLiteral')
class KafkaMonitorPlugin {

	private static final int ZK_TIMEOUT = 30000

	static void main(String[] args) {
		String zkconnect = args[0]
		List<String> topic = args[1].split(',')
		String group = args[2] ?: null

		CuratorFramework framework = createCuratorFramework(zkconnect)
		new KafkaMonitorPlugin(framework, topic, group).run()
	}

	static CuratorFramework createCuratorFramework(String zkconnect) {
		return CuratorFrameworkFactory.builder()
			.connectString(zkconnect)
			.retryPolicy(new ExponentialBackoffRetry(1000, 3))
			.connectionTimeoutMs(ZK_TIMEOUT)
			.sessionTimeoutMs(ZK_TIMEOUT)
			.build()
	}

	CuratorFramework curator
	List<String> topics
	String group

	KafkaMonitorPlugin(CuratorFramework curator, List<String> topics, String group) {
		this.curator = curator
		this.topics = topics
		this.group = group
	}

	void run() {
		List<String> cachedTopics = currentTopics

		Timer timer = new Timer()
		timer.schedule(new TopicMonitorTask(curator, cachedTopics, group), 0, 1000)
	}

	List<String> getCurrentTopics() {
		if (topics.size() == 0) {
			return curator.children.forPath("/consumers/${group}/offsets")
		}
		return topics
	}
}
