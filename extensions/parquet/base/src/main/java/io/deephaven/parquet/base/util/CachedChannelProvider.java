/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.util.file.FileHandleAccessor;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.*;

/**
 * {@link SeekableChannelsProvider Channel provider} that will cache a bounded number of unused channels.
 */
public class CachedChannelProvider implements SeekableChannelsProvider {

    private final SeekableChannelsProvider wrappedProvider;
    private final int maximumPooledCount;

    private long logicalClock;
    private long pooledCount;

    /**
     * Invalidating a {@link CachedChannelProvider} will invalidate all the channels it has produced and force it to
     * create invalid channels in the future. This prevents creating channels to files which have been overwritten.
     */
    private boolean invalid;

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

    /**
     * Stores all channels (not just the pooled ones) created by this provider for all paths. Used for invalidating file
     * handles associated with this provider.
     */
    private final Collection<WeakReference<SeekableByteChannel>> channelList = new ArrayList<>();

    private static final int CHANNEL_LIST_CLEANUP_LIMIT = 100;

    public CachedChannelProvider(@NotNull final SeekableChannelsProvider wrappedProvider,
            final int maximumPooledCount) {
        this.wrappedProvider = wrappedProvider;
        this.maximumPooledCount = Require.gtZero(maximumPooledCount, "maximumPooledCount");
        this.invalid = false;
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        final String pathKey = path.toAbsolutePath().toString();
        final KeyedObjectHashMap<String, PerPathPool> channelPool = channelPools.get(ChannelType.Read);
        final CachedChannel result = tryGetPooledChannel(pathKey, channelPool);
        if (result != null) {
            return result.position(0);
        }
        final SeekableByteChannel newReadChannel = wrappedProvider.getReadChannel(path);
        channelCreatorHelper(path, newReadChannel);
        return new CachedChannel(newReadChannel, ChannelType.Read, pathKey);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) throws IOException {
        final String pathKey = path.toAbsolutePath().toString();
        final ChannelType channelType = append ? ChannelType.WriteAppend : ChannelType.Write;
        final KeyedObjectHashMap<String, PerPathPool> channelPool = channelPools.get(channelType);
        final CachedChannel result = tryGetPooledChannel(pathKey, channelPool);
        if (result != null) {
            // The seek isn't really necessary for append; will be at end no matter what.
            return result.position(append ? result.size() : 0);
        }
        final SeekableByteChannel newWriteChannel = wrappedProvider.getWriteChannel(path, append);
        channelCreatorHelper(path, newWriteChannel);
        return new CachedChannel(newWriteChannel, channelType, pathKey);
    }

    private void channelCreatorHelper(@NotNull final Path path, @NotNull final SeekableByteChannel newChannel)
            throws IOException {
        // If channel creator is already marked invalid, mark the new channels invalid.
        // Required because CachedChannelProvider cannot return a null channel, so it returns invalid channels.
        // TODO Should we just throw an exception here?
        if (invalid) {
            invalidateChannel(newChannel);
            return;
        }
        CachedChannelProviderTracker.getInstance().registerCachedChannelProvider(this, path.toFile());
        channelList.add(new WeakReference<>(newChannel));
        if (channelList.size() >= CHANNEL_LIST_CLEANUP_LIMIT) {
            channelList.removeIf(channelWeakRef -> channelWeakRef.get() == null);
        }
    }

    private void invalidateChannel(@NotNull final SeekableByteChannel channel) {
        // Assuming that these channels are instances of FileHandleAccessor. This will be the true if
        // "wrappedProvider" is an instance of TrackedSeekableChannelsProvider.
        assert channel instanceof FileHandleAccessor;
        ((FileHandleAccessor) channel).invalidate();
    }

    public void invalidate() {
        invalid = true;
        for (WeakReference<SeekableByteChannel> channelWeakRef : channelList) {
            final SeekableByteChannel channel = channelWeakRef.get();
            if (channel != null) {
                invalidateChannel(channel);
            }
        }
        channelList.clear();
    }

    public boolean invalid() {
        return invalid;
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
        Assert.holdsLock(this, "this");
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

    /**
     * {@link SeekableByteChannel Channel} wrapper for pooled usage.
     */
    class CachedChannel implements SeekableByteChannel {

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
        public SeekableByteChannel position(final long newPosition) throws IOException {
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
            returnPoolableChannel(this);
        }

        private void dispose() throws IOException {
            wrappedChannel.close();
        }

        @VisibleForTesting
        boolean invalid() {
            if (wrappedChannel instanceof FileHandleAccessor) {
                return ((FileHandleAccessor) wrappedChannel).invalid();
            }
            return false;
        }
    }

    /**
     * Per-path pool holder for use within a ChannelPool.
     */
    private static class PerPathPool {

        private static final RAPriQueue.Adapter<PerPathPool> RAPQ_ADAPTER = new RAPriQueue.Adapter<>() {

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
                new KeyedObjectKey.Basic<>() {

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
