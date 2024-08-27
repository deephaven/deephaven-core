//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.stream.Stream;

/**
 * {@link SeekableChannelsProvider Channel provider} that will cache a bounded number of unused channels.
 */
public class CachedChannelProvider implements SeekableChannelsProvider {

    public interface ContextHolder {
        void setContext(SeekableChannelContext channelContext);

        @FinalDefault
        default void clearContext() {
            setContext(null);
        }
    }

    private final SeekableChannelsProvider wrappedProvider;
    private final int maximumPooledCount;

    private long logicalClock;
    private long pooledCount;

    enum ChannelType {
        Read, Write, WriteAppend
    }

    private final Map<ChannelType, KeyedObjectHashMap<String, PerPathPool>> channelPools;

    {
        final Map<ChannelType, KeyedObjectHashMap<String, PerPathPool>> channelPoolsTemp =
                new EnumMap<>(ChannelType.class);
        Arrays.stream(ChannelType.values())
                .forEach(ct -> channelPoolsTemp.put(ct, new KeyedObjectHashMap<>((PerPathPool.KOHM_KEY))));
        channelPools = Collections.unmodifiableMap(channelPoolsTemp);
    }

    private final RAPriQueue<PerPathPool> releasePriority =
            new RAPriQueue<>(8, PerPathPool.RAPQ_ADAPTER, PerPathPool.class);

    public static CachedChannelProvider create(@NotNull final SeekableChannelsProvider wrappedProvider,
            final int maximumPooledCount) {
        if (wrappedProvider instanceof CachedChannelProvider) {
            throw new IllegalArgumentException("Cannot wrap a CachedChannelProvider in another CachedChannelProvider");
        }
        return new CachedChannelProvider(wrappedProvider, maximumPooledCount);
    }

    private CachedChannelProvider(@NotNull final SeekableChannelsProvider wrappedProvider,
            final int maximumPooledCount) {
        this.wrappedProvider = wrappedProvider;
        this.maximumPooledCount = Require.gtZero(maximumPooledCount, "maximumPooledCount");
    }

    @Override
    public SeekableChannelContext makeContext() {
        return wrappedProvider.makeContext();
    }

    @Override
    public SeekableChannelContext makeSingleUseContext() {
        return wrappedProvider.makeSingleUseContext();
    }

    @Override
    public boolean isCompatibleWith(@NotNull final SeekableChannelContext channelContext) {
        return wrappedProvider.isCompatibleWith(channelContext);
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        return wrappedProvider.exists(uri);
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) throws IOException {
        final String uriString = uri.toString();
        final KeyedObjectHashMap<String, PerPathPool> channelPool = channelPools.get(ChannelType.Read);
        final CachedChannel result = tryGetPooledChannel(uriString, channelPool);
        final CachedChannel channel = result == null
                ? new CachedChannel(wrappedProvider.getReadChannel(channelContext, uri), ChannelType.Read, uriString)
                : result.position(0);
        channel.setContext(channelContext);
        return channel;
    }

    @Override
    public InputStream getInputStream(final SeekableByteChannel channel, final int sizeHint) throws IOException {
        return wrappedProvider.getInputStream(channel, sizeHint);
    }

    @Override
    public final CompletableOutputStream getOutputStream(@NotNull final URI uri, final int bufferSizeHint)
            throws IOException {
        return wrappedProvider.getOutputStream(uri, bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull final URI directory) throws IOException {
        return wrappedProvider.list(directory);
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) throws IOException {
        return wrappedProvider.walk(directory);
    }

    @Nullable
    private synchronized CachedChannel tryGetPooledChannel(@NotNull final String pathKey,
            @NotNull final KeyedObjectHashMap<String, PerPathPool> channelPool) {
        final PerPathPool perPathPool = channelPool.get(pathKey);
        final CachedChannel result;
        if (perPathPool == null || perPathPool.availableChannels.isEmpty()) {
            result = null;
        } else {
            result = perPathPool.availableChannels.removeFirst();
            Assert.eqFalse(result.isOpen, "result.isOpen");
            result.isOpen = true;
            if (perPathPool.availableChannels.isEmpty()) {
                releasePriority.remove(perPathPool);
            }
            --pooledCount;
        }
        return result;
    }

    private synchronized void returnPoolableChannel(@NotNull final CachedChannel cachedChannel) throws IOException {
        Assert.eqFalse(cachedChannel.isOpen, "cachedChannel.isOpen");
        cachedChannel.closeTime = advanceClock();
        if (pooledCount == maximumPooledCount) {
            final PerPathPool oldestClosedNonEmpty = releasePriority.removeTop();
            oldestClosedNonEmpty.availableChannels.removeLast().dispose();
            if (!oldestClosedNonEmpty.availableChannels.isEmpty()) {
                releasePriority.enter(oldestClosedNonEmpty);
            }
            // Conservation of pooled quantity; pooledCount does not change
        } else {
            ++pooledCount;
        }
        final PerPathPool perPathPool = channelPools.get(cachedChannel.channelType)
                .putIfAbsent(cachedChannel.pathKey,
                        pk -> new PerPathPool(cachedChannel.channelType, cachedChannel.pathKey));
        perPathPool.availableChannels.addFirst(cachedChannel);
        releasePriority.enter(perPathPool);
    }

    private long advanceClock() {
        Assert.assertion(Thread.holdsLock(this), "Thread.holdsLock(this)");
        final long newClock = ++logicalClock;
        if (newClock > 0) {
            return newClock;
        }
        // This is pretty unlikely, but reset to empty if it happens
        channelPools.values().forEach(Map::clear);
        releasePriority.clear();
        pooledCount = 0;
        return logicalClock = 1;
    }

    @Override
    public void close() {
        wrappedProvider.close();
    }

    /**
     * {@link SeekableByteChannel Channel} wrapper for pooled usage.
     */
    private class CachedChannel implements SeekableByteChannel, ContextHolder {

        private final SeekableByteChannel wrappedChannel;
        private final ChannelType channelType;
        private final String pathKey;

        private volatile boolean isOpen = true;
        private long closeTime;

        private CachedChannel(@NotNull final SeekableByteChannel wrappedChannel, @NotNull final ChannelType channelType,
                @NotNull final String pathKey) {
            this.wrappedChannel = wrappedChannel;
            this.channelType = channelType;
            this.pathKey = pathKey;
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
        public CachedChannel position(final long newPosition) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            wrappedChannel.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            return wrappedChannel.size();
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            wrappedChannel.truncate(size);
            return this;
        }

        @Override
        public String toString() {
            return pathKey;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void close() throws IOException {
            Require.eqTrue(isOpen, "isOpen");
            isOpen = false;
            clearContext();
            returnPoolableChannel(this);
        }

        private void dispose() throws IOException {
            wrappedChannel.close();
        }

        @Override
        public final void setContext(@Nullable final SeekableChannelContext channelContext) {
            if (wrappedChannel instanceof ContextHolder) {
                ((ContextHolder) wrappedChannel).setContext(channelContext);
            }
        }
    }

    /**
     * Per-path pool holder for use within a ChannelPool.
     */
    private static class PerPathPool {

        private static final RAPriQueue.Adapter<PerPathPool> RAPQ_ADAPTER = new RAPriQueue.Adapter<PerPathPool>() {

            @Override
            public boolean less(@NotNull final PerPathPool ppp1, @NotNull final PerPathPool ppp2) {
                final CachedChannel ch1 = ppp1.availableChannels.peekLast(); // Oldest channel is at the tail
                final CachedChannel ch2 = ppp2.availableChannels.peekLast();
                Assert.neq(Objects.requireNonNull(ch1).closeTime, "ch1.closeTime",
                        Objects.requireNonNull(ch2).closeTime, "ch2.closeTime");
                return ch1.closeTime < ch2.closeTime;
            }

            @Override
            public void setPos(@NotNull final PerPathPool ppp, final int slot) {
                ppp.priorityQueueSlot = slot;
            }

            @Override
            public int getPos(@NotNull final PerPathPool ppp) {
                return ppp.priorityQueueSlot;
            }
        };

        private static final KeyedObjectKey<String, PerPathPool> KOHM_KEY =
                new KeyedObjectKey.Basic<String, PerPathPool>() {

                    @Override
                    public String getKey(@NotNull final PerPathPool ppp) {
                        return ppp.path;
                    }
                };

        @SuppressWarnings({"FieldCanBeLocal", "unused"}) // Field has debugging utility
        private final ChannelType channelType;
        private final String path;

        private final Deque<CachedChannel> availableChannels = new ArrayDeque<>();

        private int priorityQueueSlot;

        private PerPathPool(@NotNull final ChannelType channelType, @NotNull final String path) {
            this.channelType = channelType;
            this.path = path;
        }
    }
}
