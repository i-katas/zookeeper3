package com.ikatas.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/**
 * @author i-katas
 * @since 1.0
 */
public class ZKServerTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();
    private final ZKServer server = new ZKServer(testFolder::newFolder);


    @Before
    public void setUp() {
        server.start();
    }

    @Test
    public void handshake() throws IOException, KeeperException, InterruptedException {
        try (ZooKeeper zk = server.connect()) {
            List<String> children = zk.getChildren("/", null);

            assertThat(children, contains("zookeeper"));
        }
    }

    @After
    public void tearDown() {
        server.stop();
    }

}
