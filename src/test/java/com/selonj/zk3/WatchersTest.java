package com.selonj.zk3;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author i-katas
 * @since 1.0
 */
public class WatchersTest extends ZooKeeperTest {
    private EventsCollector events = new EventsCollector();

    @Test
    public void watchConnectionEventsOnly() throws InterruptedException, KeeperException {
        register(events);
        assertThat(events.takeAll(), contains("SyncConnected:None:null"));

        create("/foo");
        close();
        assertThat(events.takeAll(), contains("Closed:None:null"));
    }

    @Test
    public void watchBothNodeAndChildrenCreatedEventsInfinitely() throws InterruptedException, KeeperException {
        addWatch("/foo", events, PERSISTENT_RECURSIVE);

        create("/foo", "/foo/bar", "/foo/bar/baz");

        assertThat(events.takeAll(), contains("SyncConnected:NodeCreated:/foo", "SyncConnected:NodeCreated:/foo/bar", "SyncConnected:NodeCreated:/foo/bar/baz"));
    }

    @Test
    public void watchBothNodeCreatedAndDirectChildrenChangedEventsInfinitely() throws InterruptedException, KeeperException {
        addWatch("/foo", events, AddWatchMode.PERSISTENT);

        create("/foo", "/foo/bar");
        assertThat(events.takeAll(), contains("SyncConnected:NodeCreated:/foo", "SyncConnected:NodeChildrenChanged:/foo"));

        create("/foo/bar/baz");
        assertThat(events.take(), is(nullValue()));
    }

    @Test
    public void watchDirectChildrenChangedEventOnce() throws InterruptedException, KeeperException {
        getChildren("/", events);
        create("/foo", "/bar");
        assertThat(events.takeAll(), contains("SyncConnected:NodeChildrenChanged:/"));

        getChildren("/", events);
        create("/foo/fuzz");
        assertThat(events.take(), is(nullValue()));
    }

    @Test
    public void doesNotNotifyChildrenCreatedForNodeWatches() throws InterruptedException, KeeperException {
        exists("/", events);

        create("/foo", "/bar");

        assertThat(events.takeAll(), is(empty()));
    }

    @Test
    public void notifyDataWatchRemoved() throws InterruptedException, KeeperException {
        exists("/", events);

        removeAllWatches("/");

        assertThat(events.takeAll(), contains("SyncConnected:DataWatchRemoved:/"));
    }

    @Test
    public void notifyChildrenWatchRemoved() throws InterruptedException, KeeperException {
        getChildren("/", events);

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
        Stat stat = exists("/foo", events);
        getData("/foo", events, stat);

        delete("/foo", stat.getVersion());

        assertThat(events.takeAll(), contains("SyncConnected:NodeDeleted:/foo"));
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
