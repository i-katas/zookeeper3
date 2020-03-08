package com.selonj.zk3;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.impl.NullMetricsProvider;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;

import static org.apache.zookeeper.server.ServerMetrics.metricsProviderInitialized;

/**
 * @author i-katas
 * @since 1.0
 */
public class ZKServer {
    private static final int NO_LIMITED = 0;
    private final Callable<File> dataDir;
    private final ServerCnxnFactory serverFactory;

    public ZKServer(int serverPort, Callable<File> dataDir) {
        this.dataDir = dataDir;
        try {
            serverFactory = ServerCnxnFactory.createFactory(serverPort, NO_LIMITED);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void start() {
        try {
            metricsProviderInitialized(new NullMetricsProvider());
            serverFactory.startup(createServer(dataDir.call()));
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private ZooKeeperServer createServer(File dataDir) {
        try {
            return new ZooKeeperServer(dataDir, dataDir, 2000);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void stop() {
        if (serverFactory != null) {
            serverFactory.shutdown();
        }
    }

    public ZooKeeper connect() throws IOException {
        return connect("/");
    }

    public ZooKeeper connect(String chroot) throws IOException {
        return new ZooKeeper("localhost:" + serverFactory.getLocalPort() + chroot, 2000, null);
    }
}
