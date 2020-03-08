package com.ikatas.zk;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author i-katas
 * @since 1.0
 */
public class WatchersTest extends ZooKeeperTest {
    private EventsCollector events = new EventsCollector();
    private ZooKeeper zk;

    @Test
    public void watchConnectionEventsOnly() throws InterruptedException, KeeperException {
        zk.register(events);
        assertThat(events.takeAll(), contains("SyncConnected:None:null"));

        create("/foo");
        zk.close();
        assertThat(events.takeAll(), contains("Closed:None:null"));
    }

    @Test
    public void watchBothNodeAndChildrenCreatedEventsInfinitely() throws InterruptedException, KeeperException {
        zk.addWatch("/foo", events, PERSISTENT_RECURSIVE);

        create("/foo", "/foo/bar", "/foo/bar/baz");

        assertThat(events.takeAll(), contains("SyncConnected:NodeCreated:/foo", "SyncConnected:NodeCreated:/foo/bar", "SyncConnected:NodeCreated:/foo/bar/baz"));
    }

    @Test
    public void watchBothNodeCreatedAndDirectChildrenChangedEventsInfinitely() throws InterruptedException, KeeperException {
        zk.addWatch("/foo", events, AddWatchMode.PERSISTENT);

        create("/foo", "/foo/bar");
        assertThat(events.takeAll(), contains("SyncConnected:NodeCreated:/foo", "SyncConnected:NodeChildrenChanged:/foo"));

        create("/foo/bar/baz");
        assertThat(events.take(), is(nullValue()));
    }

    @Test
    public void watchDirectChildrenChangedEventOnce() throws InterruptedException, KeeperException {
        zk.getChildren("/", events);
        create("/foo", "/bar");
        assertThat(events.takeAll(), contains("SyncConnected:NodeChildrenChanged:/"));

        zk.getChildren("/", events);
        create("/foo/fuzz");
        assertThat(events.take(), is(nullValue()));
    }

    @Test
    public void doesNotNotifyChildrenCreatedForNodeWatches() throws InterruptedException, KeeperException {
        zk.exists("/", events);

        create("/foo", "/bar");

        assertThat(events.takeAll(), is(empty()));
    }

    @Test
    public void notifyDataWatchRemoved() throws InterruptedException, KeeperException {
        zk.exists("/", events);

        removeAllWatches("/");

        assertThat(events.takeAll(), contains("SyncConnected:DataWatchRemoved:/"));
    }

    @Test
    public void notifyChildrenWatchRemoved() throws InterruptedException, KeeperException {
        zk.getChildren("/", events);

        removeAllWatches("/");

        assertThat(events.takeAll(), contains("SyncConnected:ChildWatchRemoved:/"));
    }

    @Test
    public void notifyPersistentWatchRemoved() throws InterruptedException, KeeperException {
        addWatch("/", events, AddWatchMode.PERSISTENT);

        removeAllWatches("/");

        assertThat(events.takeAll(), contains("SyncConnected:PersistentWatchRemoved:/"));
    }

    @Test
    public void notifyPersistentRecursiveWatchRemoved() throws InterruptedException, KeeperException {
        addWatch("/", events, AddWatchMode.PERSISTENT_RECURSIVE);

        removeAllWatches("/");

        assertThat(events.takeAll(), contains("SyncConnected:PersistentWatchRemoved:/"));
    }

    @Test
    public void notifyDuplicatedWatchesOnce() throws InterruptedException, KeeperException {
        create("/foo");
        Stat stat = zk.exists("/foo", events);
        zk.getData("/foo", events, stat);

        zk.delete("/foo", stat.getVersion());

        assertThat(events.takeAll(), contains("SyncConnected:NodeDeleted:/foo"));
    }

    @Override
    protected void setUp() throws IOException {
        this.zk = server.connect();
    }

    public void create(String... paths) throws KeeperException, InterruptedException {
        create(OPEN_ACL_UNSAFE, paths);
    }

    public void create(List<ACL> acl, String... paths) throws KeeperException, InterruptedException {
        for (String path : paths) {
            assertThat(zk.create(path, null, acl, PERSISTENT), equalTo(path));
        }
    }

    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode) throws KeeperException, InterruptedException {
        zk.addWatch(basePath, watcher, mode);
    }

    public void removeAllWatches(String path) {
        for (Watcher.WatcherType type : Watcher.WatcherType.values()) {
            try {
                zk.removeAllWatches(path, type, true);
            } catch (InterruptedException | KeeperException ignored) {/**/}
        }
    }


    private final static class EventsCollector implements Watcher {
        private final BlockingQueue<String> events = new ArrayBlockingQueue<>(3);
        private final int timeout = 500;
        private volatile Exception exception;

        @Override
        public void process(WatchedEvent event) {
            try {
                events.put(event.getState() + ":" + event.getType() + ":" + event.getPath());
            } catch (Exception e) {
                this.exception = e;
            }
        }

        public String take() throws InterruptedException {
            String value = events.poll(timeout, MILLISECONDS);
            if (exception != null) {
                throw new CompletionException(exception);
            }
            return value;
        }

        public List<String> takeAll() throws InterruptedException {
            List<String> values = new ArrayList<>();
            while (true) {
                String value = take();
                if (value == null) {
                    return values;
                }
                values.add(value);
            }
        }
    }
}
