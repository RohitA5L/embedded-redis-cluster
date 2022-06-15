package redis.embedded;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisClusterTest {
	private RedisCluster cluster;

	private RedisCluster ephemeralCluster;

    @Before
    public void setUp() throws Exception {
		final List<Integer> group1 = Arrays.asList(7001, 8001);
		final List<Integer> group2 = Arrays.asList(7002, 8002);
		final List<Integer> group3 = Arrays.asList(7003, 8003);
		/*
		 * creates a cluster with quorum size of 2 and 3 replication groups,
		 * each with one master and one slave
		 */
		cluster = RedisCluster.builder()
			.serverPorts(group1).replicationGroup("master1", 1)
			.serverPorts(group2).replicationGroup("master2", 1)
			.serverPorts(group3).replicationGroup("master3", 1)
			.build();
		cluster.start();

		// and for ephemeral cluster
		ephemeralCluster = RedisCluster.builder().ephemeral()
			.replicationGroup("master1", 1)
			.replicationGroup("master2", 1)
			.replicationGroup("master3", 1)
			.build();
		ephemeralCluster.start();
    }

    @Test
    public void testRedisClusterWithPredefinedPorts() throws Exception {
		JedisCluster jc = null;

		try {
			jc = new JedisCluster(new HostAndPort("127.0.0.1", 7001));
			jc.set("somekey", "somevalue");
			assertEquals("the value shoudl be equal", "somevalue", jc.get("somekey"));
		} finally {
			if (jc != null) {
				jc.close();
			}
		}
    }

	@Test
	public void testRedisClusterWithEphemeralPorts() throws Exception {
		JedisCluster jc = null;

		try {
			jc = new JedisCluster(new HostAndPort("127.0.0.1", this.ephemeralCluster.ports().get(0)));
			jc.set("somekey", "somevalue");
			assertEquals("the value shoudl be equal", "somevalue", jc.get("somekey"));
		} finally {
			if (jc != null) {
				jc.close();
			}
		}
	}

	@Test
	public void testClusterRolesAreCorrectlyInitialized() {
		Jedis j = null;

		try {
			j = new Jedis("127.0.0.1", 7001);
			String[] clusterNodes = j.clusterNodes().split("\n");
			Map<String, Integer> nodeIdToPort = new HashMap<>();
			Map<Integer, String> replicas = new HashMap<>();
			for (String node:clusterNodes) {
				// Expected line format from Redis docs (one line - one node)
				// 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
				// 6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
				String[] parts = node.split(" ");
				if (parts.length < 3) {
					continue;
				}

				Integer nodePort = Integer.parseInt(parts[1].split("@")[0].split(":")[1]);
				List<String> nodeTags = Lists.newArrayList(parts[2].split(","));

				nodeIdToPort.put(parts[0], nodePort);

				String replicaOf = parts[3];

				if (nodePort > 8000) {
					assertTrue("Node must be a replica", nodeTags.contains("slave"));
					replicas.put(nodePort, replicaOf);

				} else {
					assertTrue(nodeTags.contains("master"));
				}
			}

			for (Map.Entry<Integer, String> replica:replicas.entrySet()) {
				// replica port - 1000 == master port
				assertEquals(
					Long.valueOf(replica.getKey() - 1000),
					Long.valueOf(nodeIdToPort.get(replica.getValue()))
				);
			}

		} finally {
			if (j != null) {
				j.close();
			}
		}
	}

    @After
    public void tearDown() throws Exception {
		this.cluster.stop();
		this.ephemeralCluster.stop();
    }
}
