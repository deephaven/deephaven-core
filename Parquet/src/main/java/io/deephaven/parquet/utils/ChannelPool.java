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

    ChannelPool(@NotNull final Function<String, CachedChannelProvider.CachedChannel> channelCreator) {
        this.channelCreator = channelCreator;
    }

    public int count() {
        return pooledCount;
    }

    static class TimeToPath implements Comparable<TimeToPath> {
        private final String path;
        private final long logicalTime;

        TimeToPath(String path, long logicalTime) {
            this.path = path;
            this.logicalTime = logicalTime;
        }

        @Override
        public int compareTo(@NotNull final TimeToPath other) {
            return (int) Math.signum(logicalTime - other.logicalTime);
        }
    }

    void pool(@NotNull final CachedChannelProvider.CachedChannel cachedChannel) {
        final Deque<CachedChannelProvider.CachedChannel> dest = pool.computeIfAbsent(cachedChannel.getPath(), (path) -> {
            thingsToRelease.add(new TimeToPath(cachedChannel.getPath(), cachedChannel.closeTime()));
            return new ArrayDeque<>();
        });
        dest.addFirst(cachedChannel);
        pooledCount++;
    }

    CachedChannelProvider.CachedChannel getChannel(@NotNull final String path, final boolean needsRelease) throws IOException {
        final Deque<CachedChannelProvider.CachedChannel> queue = pool.get(path);
        if (queue == null || queue.isEmpty()) {
            CachedChannelProvider.CachedChannel result = channelCreator.apply(path);
            if (needsRelease) {
                releaseNext();
            }
            return result;
        }
        pooledCount--;
        final CachedChannelProvider.CachedChannel cachedChannel = queue.removeFirst();
        Assert.neqTrue(cachedChannel.isOpen, "cachedChannel.isOpen");
        cachedChannel.isOpen = true;
        return cachedChannel;
    }

    void releaseNext() throws IOException {
        final ChannelPool.TimeToPath toRelease = thingsToRelease.poll();
        if (toRelease == null) {
            return;
        }
        final Deque<CachedChannelProvider.CachedChannel> cachedChannels = pool.get(toRelease.path);
        cachedChannels.removeLast().dispose();
        if (!cachedChannels.isEmpty()) {
            final CachedChannelProvider.CachedChannel nextInLine = cachedChannels.peekLast();
            thingsToRelease.add(new TimeToPath(nextInLine.getPath(), nextInLine.closeTime()));
        }
        pooledCount--;
    }
}
