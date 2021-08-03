package io.deephaven.parquet.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SeekableChannelsProvider Channel provider} that will cache a bounded number of unused channels.
 */
public class CachedChannelProvider implements SeekableChannelsProvider {

    private final SeekableChannelsProvider wrappedProvider;
    private final int maxSize;

    private final AtomicLong logicalClock = new AtomicLong(0);

    enum ChannelType {
        Read, Write, WriteAppend
    }

    private final Map<ChannelType, ChannelPool> pools;
    {
        final Map<ChannelType, ChannelPool> poolsTemp = new EnumMap<>(ChannelType.class);
        Arrays.stream(ChannelType.values()).forEach(ct -> poolsTemp.put(ct, new ChannelPool()));
        pools = Collections.unmodifiableMap(poolsTemp);
    }

    public CachedChannelProvider(@NotNull final SeekableChannelsProvider wrappedProvider, final int maxSize) {
        this.wrappedProvider = wrappedProvider;
        this.maxSize = maxSize;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        final String pathKey = path.toAbsolutePath().toString();
        final ChannelPool channelPool = pools.get(ChannelType.Read);
        final SeekableByteChannel result;
        synchronized (this) {
            result = getSeekableByteChannel(pathKey, totalPooledCount() >= maxSize, channelPool);
        }
        return result == null ? new CachedChannel(wrappedProvider.getReadChannel(path), ChannelType.Read, pathKey) : result.position(0);
    }

    @Override
    public synchronized SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) throws IOException {
        final String pathKey = path.toAbsolutePath().toString();
        final ChannelType channelType = append ? ChannelType.WriteAppend : ChannelType.Write;
        final ChannelPool channelPool = pools.get(channelType);
        final SeekableByteChannel result;
        synchronized (this) {
            result = getSeekableByteChannel(pathKey, totalPooledCount() >= maxSize, channelPool);
        }
        return result == null
                ? new CachedChannel(wrappedProvider.getWriteChannel(path, append), channelType, pathKey)
                : result.position(append ? result.size() : 0); // The seek isn't really necessary for append; will be at end no matter what.
    }

    private int totalPooledCount() {
        int sum = 0;
        for (final ChannelPool channelPool : pools.values()) {
            sum += channelPool.pooledCount();
        }
        return sum;
    }

    @Nullable
    private SeekableByteChannel getSeekableByteChannel(@NotNull final String pathKey,
                                                       final boolean needsRelease,
                                                       @NotNull final ChannelPool channelPool) throws IOException {
        final boolean isReleasing = needsRelease && channelPool.pooledCount() > 0;
        final CachedChannel result = channelPool.take(pathKey, isReleasing);
        if (needsRelease && !isReleasing) {
            releaseOther();
        }
        return result;
    }

    private void releaseOther() throws IOException {
        for (final ChannelPool pool : pools.values()) {
            if (pool.pooledCount() > 0) {
                pool.releaseNext();
                return;
            }
        }
    }

    private synchronized void give(@NotNull final CachedChannel cachedChannel, @NotNull final ChannelType channelType) throws IOException {
        pools.get(channelType).give(cachedChannel);
        if (totalPooledCount() > maxSize) {
            releaseOther();
        }
    }

    /**
     * {@link SeekableByteChannel Channel} wrapper for pooled usage.
     */
    private class CachedChannel implements SeekableByteChannel {

        private final SeekableByteChannel wrappedChannel;
        private final ChannelType channelType;
        private final String path;

        private boolean isOpen;
        private long closeTime;

        private CachedChannel(@NotNull final SeekableByteChannel wrappedChannel, @NotNull final ChannelType channelType, @NotNull final String path) {
            this.wrappedChannel = wrappedChannel;
            this.channelType = channelType;
            this.path = path;
            isOpen = true;
        }

        @Override
        public int read(@NotNull final ByteBuffer dst) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.read(dst);
        }

        @Override
        public int write(@NotNull final ByteBuffer src) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.write(src);
        }

        @Override
        public long position() throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.size();
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.truncate(size);
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void close() throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            closeTime = logicalClock.incrementAndGet();
            isOpen = false;
            give(this, channelType);
        }

        public String getPath() {
            return path;
        }

        private long closeTime() {
            return closeTime;
        }

        public void dispose() throws IOException {
            wrappedChannel.close();
        }
    }

    /**
     * Per-{@link ChannelType type} pool.
     */
    private static class ChannelPool {

        private final Map<String, Deque<CachedChannel>> pool = new HashMap<>();
        private final PriorityQueue<TimeToPath> thingsToRelease = new PriorityQueue<>();

        private int pooledCount = 0;

        private ChannelPool() {
        }

        private int pooledCount() {
            return pooledCount;
        }

        private static class TimeToPath implements Comparable<TimeToPath> {

            private final String path;
            private final long logicalTime;

            TimeToPath(String path, long logicalTime) {
                this.path = path;
                this.logicalTime = logicalTime;
            }

            @Override
            public int compareTo(@NotNull final TimeToPath other) {
                return Long.compare(logicalTime, other.logicalTime);
            }
        }

        private void give(@NotNull final CachedChannel cachedChannel) {
            Assert.neqTrue(cachedChannel.isOpen, "cachedChannel.isOpen");
            final Deque<CachedChannel> dest = pool.computeIfAbsent(cachedChannel.getPath(), (path) -> {
                thingsToRelease.add(new TimeToPath(cachedChannel.getPath(), cachedChannel.closeTime()));
                return new ArrayDeque<>();
            });
            dest.addFirst(cachedChannel);
            pooledCount++;
        }

        @Nullable
        private CachedChannel take(@NotNull final String path, final boolean needsRelease) throws IOException {
            final Deque<CachedChannel> queue = pool.get(path);
            if (queue == null || queue.isEmpty()) {
                if (needsRelease) {
                    releaseNext();
                }
                return null;
            }
            pooledCount--;
            final CachedChannel cachedChannel = queue.removeFirst();
            Assert.neqTrue(cachedChannel.isOpen, "cachedChannel.isOpen");
            cachedChannel.isOpen = true;
            return cachedChannel;
        }

        private void releaseNext() throws IOException {
            final TimeToPath toRelease = thingsToRelease.poll();
            if (toRelease == null) {
                return;
            }
            final Deque<CachedChannel> cachedChannels = pool.get(toRelease.path);
            cachedChannels.removeLast().dispose();
            if (!cachedChannels.isEmpty()) {
                final CachedChannel nextInLine = cachedChannels.peekLast();
                thingsToRelease.add(new TimeToPath(nextInLine.getPath(), nextInLine.closeTime()));
            }
            pooledCount--;
        }
    }
}
