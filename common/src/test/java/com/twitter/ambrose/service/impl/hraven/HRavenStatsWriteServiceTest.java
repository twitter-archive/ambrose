package com.twitter.ambrose.service.impl.hraven;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for HRavenStatsWriteService.
 */
public class HRavenStatsWriteServiceTest {
  @Test
  public void testInitHBaseConfiguration() {
    Configuration tmp = new Configuration();
    String zkServers = "zk1,zk2,zk3";
    String zkPort = "8181";
    String parentZnode = "/hraven-test";

    tmp.set(HConstants.ZOOKEEPER_QUORUM, zkServers);
    tmp.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
    tmp.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parentZnode);
    String zkConfigString = ZKUtil.getZooKeeperClusterKey(tmp);

    Configuration jobConf = new Configuration();
    jobConf.set(HRavenStatsWriteService.HRAVEN_ZOOKEEPER_QUORUM, zkConfigString);

    Configuration hravenConfig = HRavenStatsWriteService.initHBaseConfiguration(jobConf);
    assertEquals(zkServers, hravenConfig.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkPort, hravenConfig.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    assertEquals(parentZnode, hravenConfig.get(HConstants.ZOOKEEPER_ZNODE_PARENT));

    String zkServers2 = "zk4,zk5,zk6";
    String zkPort2 = "9191";
    String parentZnode2 = "/hraven-test2";

    tmp = new Configuration();
    tmp.set(HConstants.ZOOKEEPER_QUORUM, zkServers2);
    tmp.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort2);
    tmp.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parentZnode2);

    String zkConfigString2 = ZKUtil.getZooKeeperClusterKey(tmp);
    // check overriding the previously set values
    hravenConfig.set(HRavenStatsWriteService.HRAVEN_ZOOKEEPER_QUORUM, zkConfigString2);

    Configuration hravenConfig2 = HRavenStatsWriteService.initHBaseConfiguration(hravenConfig);
    assertEquals(zkServers2, hravenConfig2.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkPort2, hravenConfig2.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    assertEquals(parentZnode2, hravenConfig2.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
  }
}
