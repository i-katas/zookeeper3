package com.ikatas.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author i-katas
 * @since 1.0
 */
public class EphemeralNodeTest extends ZooKeeperTest {
    private ZooKeeper client1;
    private ZooKeeper client2;

    public void setUp() throws Exception {
        client1 = server.connect();
        client2 = server.connect();
    }

    @Test
    public void deleteEphemeralNodesWhenSessionClosed() throws KeeperException, InterruptedException {
        client1.create("/node", null, OPEN_ACL_UNSAFE, EPHEMERAL);
        assertThat(client2.exists("/node", false), is(not(nullValue())));

        client1.close();

        assertThat(client2.exists("/node", false), is(nullValue()));
    }

    @Override
    protected void tearDown() throws Exception {
        client1.close();
        client2.close();
    }
}
