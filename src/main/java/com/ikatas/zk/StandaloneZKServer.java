package com.ikatas.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.impl.NullMetricsProvider;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Stream.empty;
import static org.apache.zookeeper.server.ServerMetrics.metricsProviderInitialized;

/**
 * @author i-katas
 * @since 1.0
 */
public class StandaloneZKServer implements ZKServer {
    private static final int NO_LIMITED = Integer.MAX_VALUE;
    private final ServerCnxnFactory serverFactory;
    private final Callable<File> dataDir;

    public StandaloneZKServer(int serverPort, Callable<File> dataDir) {
        try {
            this.serverFactory = ServerCnxnFactory.createFactory(serverPort, NO_LIMITED);
            this.dataDir = dataDir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void start() {
        try {
            metricsProviderInitialized(new NullMetricsProvider());
            serverFactory.startup(createServer(dataDir.call()));
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private ZooKeeperServer createServer(File dataDir) throws IOException {
        return new ZooKeeperServer(dataDir, dataDir, 2000);
    }

    @Override
    public void stop() {
        if (serverFactory != null) {
            serverFactory.shutdown();
        }
    }

    @Override
    public CompletableFuture<Void> removeRecursively(String path) throws IOException {
        ZooKeeper zk = connect();
        return removeRecursively(zk, path).thenRun(disconnect(zk));
    }

    private Runnable disconnect(ZooKeeper zk) {
        return () -> {
            try {
                zk.close();
            } catch (InterruptedException ignored) {/**/}
        };
    }

    @Override
    public CompletableFuture<Void> removeRecursively(Stream<String> paths) throws IOException {
        ZooKeeper zk = connect();
        return removeRecursively(zk, paths).thenRun(disconnect(zk));
    }

    private CompletableFuture<Void> removeRecursively(ZooKeeper zk, String path) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.getChildren(path, false, (rc, parent, ctx, children, stat) -> {
            Stream<String> toRemovePaths = children == null ? empty() : children.stream().map(child -> parent + "/" + child);
            removeRecursively(zk, toRemovePaths).whenComplete((value, ex) -> {
                if (ex != null) {
                    promise.completeExceptionally(ex);
                    return;
                }
                try {
                    if (stat != null) {
                        zk.delete(parent, stat.getVersion());
                    }
                    promise.complete(null);
                } catch (KeeperException.NoNodeException ignored) {
                    promise.complete(null);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            });
        }, null);
        return promise;
    }

    private CompletableFuture<Void> removeRecursively(ZooKeeper zk, Stream<String> paths) {
        return allOf(paths.map(path -> removeRecursively(zk, path)).toArray(CompletableFuture[]::new));
    }

    @Override
    public ZooKeeper connect() throws IOException {
        return connect("/");
    }

    @Override
    public ZooKeeper connect(String chroot) throws IOException {
        return new ZooKeeper("localhost:" + serverFactory.getLocalPort() + chroot, 2000, null);
    }
}
