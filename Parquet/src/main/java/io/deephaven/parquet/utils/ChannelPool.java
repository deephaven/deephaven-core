package io.deephaven.parquet.utils;

import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;


public class ChannelPool {

    private final Function<String, CachedChannelProvider.CachedChannel> channelCreator;

    private final Map<String, Deque<CachedChannelProvider.CachedChannel>> pool = new HashMap<>();

    private final PriorityQueue<TimeToPath> thingsToRelease = new PriorityQueue<>();
    private int pooledCount = 0;

    ChannelPool(Function<String, CachedChannelProvider.CachedChannel> channelCreator) {
        this.channelCreator = channelCreator;
    }

    public int count() {
        return pooledCount;
    }

    static class TimeToPath implements Comparable<TimeToPath> {
        final String path;
        final long logicalTime;

        TimeToPath(String path, long logicalTime) {
            this.path = path;
            this.logicalTime = logicalTime;
        }

        @Override
        public int compareTo(@NotNull TimeToPath o) {
            return (int) Math.signum(logicalTime - o.logicalTime);
        }

    }


    void pool(CachedChannelProvider.CachedChannel cachedChannel) {
        Deque<CachedChannelProvider.CachedChannel> dest = pool.computeIfAbsent(cachedChannel.getPath(), (path) -> {
            thingsToRelease.add(new TimeToPath(cachedChannel.getPath(), cachedChannel.closeTime()));
            return new ArrayDeque<>();
        });
        dest.addFirst(cachedChannel);
        pooledCount++;
    }

    CachedChannelProvider.CachedChannel getChannel(String path, boolean needsRelease) throws IOException {
        Deque<CachedChannelProvider.CachedChannel> queue = pool.get(path);
        if (queue == null || queue.isEmpty()) {
            CachedChannelProvider.CachedChannel result = channelCreator.apply(path);
            if (needsRelease) {
                releaseNext();
            }
            return result;
        }
        pooledCount--;
        CachedChannelProvider.CachedChannel cachedChannel = queue.removeFirst();
        Assert.neqTrue(cachedChannel.isOpen,"cachedChannel.isOpen");
        cachedChannel.isOpen = true;
        return cachedChannel;
    }

    void releaseNext() throws IOException {
        Deque<CachedChannelProvider.CachedChannel> cachedChannels = pool.get(thingsToRelease.poll().path);
        cachedChannels.removeLast().dispose();
        if (!cachedChannels.isEmpty()) {
            CachedChannelProvider.CachedChannel nextInLine = cachedChannels.peekLast();
            thingsToRelease.add(new TimeToPath(nextInLine.getPath(),nextInLine.closeTime()));
        }
        pooledCount--;
    }

}
