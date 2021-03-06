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
public class ZKServer {
    private static final int NO_LIMITED = 0;
    private static final int RANDOM_PORT = 0;
    private final ServerCnxnFactory serverFactory;
    private final Callable<File> dataDir;
    private boolean started;
    private ZooKeeperServer server;

    public ZKServer(Callable<File> dataDir) {
        this(RANDOM_PORT, dataDir);
    }

    public ZKServer(int serverPort, Callable<File> dataDir) {
        try {
            this.serverFactory = ServerCnxnFactory.createFactory(serverPort, NO_LIMITED);
            this.dataDir = dataDir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void start() {
        if (started) {
            return;
        }
        try {
            started = true;
            metricsProviderInitialized(new NullMetricsProvider());
            server = createServer(dataDir.call());
            serverFactory.startup(server);
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private ZooKeeperServer createServer(File dataDir) throws IOException {
        return new ZooKeeperServer(dataDir, dataDir, 2000);
    }

    public void stop() {
        if (serverFactory != null) {
            server.shutdown(true);
            serverFactory.shutdown();
            started = false;
        }
    }

    public CompletableFuture<Void> removeRecursively(String path) throws IOException {
        ZooKeeper connection = connect();
        return removeRecursively(connection, path).thenRun(disconnect(connection));
    }

    public CompletableFuture<Void> removeRecursively(Stream<String> paths) throws IOException {
        ZooKeeper connection = connect();
        return removeRecursively(connection, paths).thenRun(disconnect(connection));
    }

    private Runnable disconnect(ZooKeeper zk) {
        return () -> {
            try {
                zk.close();
            } catch (InterruptedException ignored) {/**/}
        };
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

    public ZooKeeper connect() throws IOException {
        return connect("/");
    }

    public ZooKeeper connect(String chroot) throws IOException {
        if (!started) {
            throw new IllegalStateException("Server has not been started!");
        }
        return new ZooKeeper("localhost:" + serverFactory.getLocalPort() + chroot, 2000, null);
    }
}
