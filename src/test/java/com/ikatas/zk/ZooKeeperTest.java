package com.ikatas.zk;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * @author i-katas
 * @since 1.0
 */
public abstract class ZooKeeperTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();
    public final ZKServer server = ZKServer.continuous(2181, testFolder::newFolder);


    @Before
    public final void startServer() throws Exception {
        server.start();
        setUp();
    }

    protected void setUp() throws IOException {
    }


    @After
    public final void stopServer() throws Exception {
        server.stop();
    }
}
