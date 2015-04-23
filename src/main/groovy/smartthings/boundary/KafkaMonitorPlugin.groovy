package smartthings.boundary

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import smartthings.boundary.client.Client
import smartthings.boundary.client.InfluxDbClient

@SuppressWarnings('DuplicateNumberLiteral')
class KafkaMonitorPlugin {

	private static final int ZK_TIMEOUT = 1000

	static void main(String[] args) {
		ConfigObject config = new ConfigSlurper().parse(new File(args[0]).toURL())

		Client client = new InfluxDbClient(
				config.influxdb.connect,
				config.influxdb.dbName,
				config.influxdb.username,
				config.influxdb.password
		)

		CuratorFramework framework = createCuratorFramework(config.zkconnect)
		new KafkaMonitorPlugin(framework, client, (List<String>) config.topic.split(','), config.group).run()
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
	Client client
	List<String> topics
	String group

	KafkaMonitorPlugin(CuratorFramework curator, Client client, List<String> topics, String group) {
		this.curator = curator
		this.client = client
		this.topics = topics
		this.group = group
	}

	void run() {
		curator.start()

		Timer timer = new Timer()
		timer.schedule(new TopicMonitorTask(curator, client, currentTopics, group), 0, 1000)
	}

	List<String> getCurrentTopics() {
		if (topics.size() == 0) {
			return curator.children.forPath("/consumers/${group}/offsets")
		}
		return topics
	}
}
