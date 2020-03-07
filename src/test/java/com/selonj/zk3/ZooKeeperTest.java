package com.selonj.zk3;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Testable;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Stream.empty;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author i-katas
 * @since 1.0
 */
public abstract class ZooKeeperTest {
    private ZooKeeper zk;
    private final String connection;
    private final int sessionTimeout;

    public ZooKeeperTest() {
        this("localhost:2181", 2000);
    }

    public ZooKeeperTest(String connection, int sessionTimeout) {
        this.connection = connection;
        this.sessionTimeout = sessionTimeout;
    }

    @Before
    public final void setUp() throws Exception {
        String testRoot = "/test";
        try (@SuppressWarnings("unused") ZooKeeper starter = this.zk = connect("/")) {
            removeRecursively(testRoot).join();
            create(testRoot);
        }
        this.zk = connect(testRoot);
    }

    private ZooKeeper connect(String chroot) throws IOException {
        return new ZooKeeper(connection + chroot, sessionTimeout, null);
    }

    public CompletableFuture<Void> removeRecursively(String path) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        zk.getChildren(path, false, (rc, parent, ctx, children, stat) -> {
            Stream<String> toRemovePaths = children == null ? empty() : children.stream().map(child -> parent + "/" + child);
            removeRecursively(toRemovePaths).whenComplete((value, ex) -> {
                if (ex != null) {
                    promise.completeExceptionally(ex);
                    return;
                }
                try {
                    if (stat != null) {
                        zk.delete(parent, stat.getVersion());
                    }
                    promise.complete(null);
                } catch (NoNodeException ignored) {
                    promise.complete(null);
                } catch (Exception e) {
                    promise.completeExceptionally(e);
                }
            });
        }, null);
        return promise;
    }

    public CompletableFuture<Void> removeRecursively(Stream<String> paths) {
        return allOf(paths.map(this::removeRecursively).toArray(CompletableFuture[]::new));
    }

    public void create(String... paths) throws KeeperException, InterruptedException {
        create(ZooDefs.Ids.OPEN_ACL_UNSAFE, paths);
    }

    public void create(List<ACL> acl, String... paths) throws KeeperException, InterruptedException {
        for (String path : paths) {
            assertThat(zk.create(path, null, acl, PERSISTENT), equalTo(path));
        }
    }

    public void updateServerList(String connectString) throws IOException {
        zk.updateServerList(connectString);
    }

    public ZooKeeperSaslClient getSaslClient() {
        return zk.getSaslClient();
    }

    public ZKClientConfig getClientConfig() {
        return zk.getClientConfig();
    }

    public Testable getTestable() {
        return zk.getTestable();
    }

    public long getSessionId() {
        return zk.getSessionId();
    }

    public byte[] getSessionPasswd() {
        return zk.getSessionPasswd();
    }

    public int getSessionTimeout() {
        return zk.getSessionTimeout();
    }

    public void addAuthInfo(String scheme, byte[] auth) {
        zk.addAuthInfo(scheme, auth);
    }

    public void register(Watcher watcher) {
        zk.register(watcher);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public boolean close(int waitForShutdownTimeoutMs) throws InterruptedException {
        return zk.close(waitForShutdownTimeoutMs);
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        return zk.create(path, data, acl, createMode);
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat) throws KeeperException, InterruptedException {
        return zk.create(path, data, acl, createMode, stat);
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws KeeperException, InterruptedException {
        return zk.create(path, data, acl, createMode, stat, ttl);
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        zk.create(path, data, acl, createMode, cb, ctx);
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.Create2Callback cb, Object ctx) {
        zk.create(path, data, acl, createMode, cb, ctx);
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.Create2Callback cb, Object ctx, long ttl) {
        zk.create(path, data, acl, createMode, cb, ctx, ttl);
    }

    public void delete(String path, int version) throws InterruptedException, KeeperException {
        zk.delete(path, version);
    }

    public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
        return zk.multi(ops);
    }

    public void multi(Iterable<Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
        zk.multi(ops, cb, ctx);
    }

    public Transaction transaction() {
        return zk.transaction();
    }

    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.delete(path, version, cb, ctx);
    }

    public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zk.exists(path, watcher);
    }

    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return zk.exists(path, watch);
    }

    public void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
        zk.exists(path, watcher, cb, ctx);
    }

    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
        zk.exists(path, watch, cb, ctx);
    }

    public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return zk.getData(path, watcher, stat);
    }

    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return zk.getData(path, watch, stat);
    }

    public void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        zk.getData(path, watcher, cb, ctx);
    }

    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
        zk.getData(path, watch, cb, ctx);
    }

    public byte[] getConfig(Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return zk.getConfig(watcher, stat);
    }

    public void getConfig(Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        zk.getConfig(watcher, cb, ctx);
    }

    public byte[] getConfig(boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return zk.getConfig(watch, stat);
    }

    public void getConfig(boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
        zk.getConfig(watch, cb, ctx);
    }

    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return zk.setData(path, data, version);
    }

    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        zk.setData(path, data, version, cb, ctx);
    }

    public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
        return zk.getACL(path, stat);
    }

    public void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
        zk.getACL(path, stat, cb, ctx);
    }

    public Stat setACL(String path, List<ACL> acl, int aclVersion) throws KeeperException, InterruptedException {
        return zk.setACL(path, acl, aclVersion);
    }

    public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
        zk.setACL(path, acl, version, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watcher);
    }

    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb, Object ctx) {
        zk.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
        zk.getChildren(path, watch, cb, ctx);
    }

    public List<String> getChildren(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watcher, stat);
    }

    public List<String> getChildren(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch, stat);
    }

    public void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
        zk.getChildren(path, watcher, cb, ctx);
    }

    public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
        zk.getChildren(path, watch, cb, ctx);
    }

    public int getAllChildrenNumber(String path) throws KeeperException, InterruptedException {
        return zk.getAllChildrenNumber(path);
    }

    public void getAllChildrenNumber(String path, AsyncCallback.AllChildrenNumberCallback cb, Object ctx) {
        zk.getAllChildrenNumber(path, cb, ctx);
    }

    public List<String> getEphemerals() throws KeeperException, InterruptedException {
        return zk.getEphemerals();
    }

    public List<String> getEphemerals(String prefixPath) throws KeeperException, InterruptedException {
        return zk.getEphemerals(prefixPath);
    }

    public void getEphemerals(String prefixPath, AsyncCallback.EphemeralsCallback cb, Object ctx) {
        zk.getEphemerals(prefixPath, cb, ctx);
    }

    public void getEphemerals(AsyncCallback.EphemeralsCallback cb, Object ctx) {
        zk.getEphemerals(cb, ctx);
    }

    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.sync(path, cb, ctx);
    }

    public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local) throws InterruptedException, KeeperException {
        zk.removeWatches(path, watcher, watcherType, local);
    }

    public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.removeWatches(path, watcher, watcherType, local, cb, ctx);
    }

    public void removeAllWatches(String path, Watcher.WatcherType watcherType, boolean local) throws InterruptedException, KeeperException {
        zk.removeAllWatches(path, watcherType, local);
    }

    public void removeAllWatches(String path, Watcher.WatcherType watcherType, boolean local, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.removeAllWatches(path, watcherType, local, cb, ctx);
    }

    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode) throws KeeperException, InterruptedException {
        zk.addWatch(basePath, watcher, mode);
    }

    public void addWatch(String basePath, AddWatchMode mode) throws KeeperException, InterruptedException {
        zk.addWatch(basePath, mode);
    }

    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.addWatch(basePath, watcher, mode, cb, ctx);
    }

    public void addWatch(String basePath, AddWatchMode mode, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.addWatch(basePath, mode, cb, ctx);
    }

    public ZooKeeper.States getState() {
        return zk.getState();
    }

    public void removeAllWatches(String path) {
        for (Watcher.WatcherType type : Watcher.WatcherType.values()) {
            try {
                zk.removeAllWatches(path, type, true);
            } catch (InterruptedException | KeeperException ignored) {/**/}
        }
    }

    @After
    public final void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
    }
}
