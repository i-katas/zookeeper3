package com.ikatas.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author i-katas
 * @since 1.0
 */
public interface ZKServer {
    static ZKServer continuous(int port, Callable<File> dataDir) {
        return ContinuousZKServer.of(port, dataDir);
    }

    void start();

    void stop();

    CompletableFuture<Void> removeRecursively(String path) throws IOException;

    CompletableFuture<Void> removeRecursively(Stream<String> paths) throws IOException;

    default ZooKeeper connect() throws IOException {
        return connect("/");
    }

    ZooKeeper connect(String chroot) throws IOException;

    default void setStopAtShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

}

class ContinuousZKServer implements ZKServer {
    private static final Map<Integer, ZKServer> created = new ConcurrentHashMap<>();
    private final StandaloneZKServer server;
    private boolean started = false;

    public ContinuousZKServer(StandaloneZKServer server) {
        this.server = server;
    }

    public static ZKServer of(int port, Callable<File> dataDir) {
        created.computeIfAbsent(port, (serverPort) -> new ContinuousZKServer(new StandaloneZKServer(port, dataDir)));
        return created.get(port);
    }

    @Override
    public void start() {
        if (!started) {
            started = true;
            server.start();
            setStopAtShutdown();
        }
        try (ZooKeeper zk = connect()) {
            Stream<String> toRemovePaths = zk.getChildren("/", null).stream().filter(path -> !path.equals("zookeeper")).map(path -> "/" + path);
            removeRecursively(toRemovePaths).join();
        } catch (InterruptedException | IOException | KeeperException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public CompletableFuture<Void> removeRecursively(String path) throws IOException {
        return server.removeRecursively(path);
    }

    @Override
    public CompletableFuture<Void> removeRecursively(Stream<String> paths) throws IOException {
        return server.removeRecursively(paths);
    }

    @Override
    public ZooKeeper connect(String chroot) throws IOException {
        return server.connect(chroot);
    }
}
