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
    public final ZKServer server = new ZKServer(testFolder::newFolder);


    @Before
    public final void startServer() throws Exception {
        server.start();
        setUp();
    }

    protected void setUp() throws IOException, Exception {
    }


    @After
    public final void stopServer() throws Exception {
        tearDown();
        server.stop();
    }

    protected void tearDown() throws Exception {
    }
}
