package io.deephaven.parquet.utils;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CachedChannelProvider implements SeekableChannelsProvider {

    private final AtomicLong logicalClock = new AtomicLong(0);
    private final Map<ChannelType, ChannelPool> pools = new HashMap<>();
    private final int maxSize;

    public CachedChannelProvider(@NotNull final SeekableChannelsProvider wrappedProvider, final int maxSize) {
        this.maxSize = maxSize;
        pools.put(ChannelType.Read, new ChannelPool((path) -> {
            String absolutePath = Paths.get(path).toAbsolutePath().toString();
            try {
                return new CachedChannel(wrappedProvider.getReadChannel(path), ChannelType.Read, absolutePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        pools.put(ChannelType.Write, new ChannelPool((path) -> {
            String absolutePath = Paths.get(path).toAbsolutePath().toString();
            try {
                return new CachedChannel(wrappedProvider.getWriteChannel(path, false), ChannelType.Write, absolutePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        pools.put(ChannelType.WriteAppend, new ChannelPool((path) -> {
            String absolutePath = Paths.get(path).toAbsolutePath().toString();
            try {
                return new CachedChannel(wrappedProvider.getWriteChannel(path, true), ChannelType.WriteAppend, absolutePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        final boolean needsRelease = poolSize() >= maxSize;
        final ChannelPool channelPool = pools.get(ChannelType.Read);
        final SeekableByteChannel seekableByteChannel = getSeekableByteChannel(path, needsRelease, channelPool);
        seekableByteChannel.position(0);
        return seekableByteChannel;
    }

    private synchronized SeekableByteChannel getSeekableByteChannel(@NotNull final Path path, final boolean needsRelease,
                                                                    @NotNull final ChannelPool channelPool) throws IOException {
        final boolean isReleasing = needsRelease && channelPool.count() > 0;
        final CachedChannel result = channelPool.getChannel(path.toAbsolutePath().toString(), isReleasing);
        if (needsRelease && !isReleasing) {
            releaseOther();
        }
        return result;
    }

    @Override
    public synchronized SeekableByteChannel getWriteChannel(@NotNull final Path filePath, final boolean append) throws IOException {
        final boolean needsRelease = poolSize() >= maxSize;
        final ChannelPool channelPool;
        if (append) {
            channelPool = pools.get(ChannelType.WriteAppend);
        } else {
            channelPool = pools.get(ChannelType.Write);
        }
        final SeekableByteChannel result = getSeekableByteChannel(filePath, needsRelease, channelPool);
        if (append) {
            result.position(result.size());
        } else {
            result.position(0);
        }
        return result;
    }

    private void releaseOther() throws IOException {
        for (ChannelPool pool : pools.values()) {
            if (pool.count() > 0) {
                pool.releaseNext();
                return;
            }
        }
    }

    private int poolSize() {
        return pools.values().stream().mapToInt(ChannelPool::count).sum();
    }

    enum ChannelType {
        Read, Write, WriteAppend
    }

    class CachedChannel implements SeekableByteChannel {

        private final SeekableByteChannel wrappedChannel;
        private final ChannelType channelType;
        private final String path;

        boolean isOpen;
        private long closeTime;

        CachedChannel(@NotNull final SeekableByteChannel wrappedChannel, @NotNull final ChannelType channelType, @NotNull final String path) {
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
            pool(this, channelType);
        }

        public String getPath() {
            return path;
        }

        long closeTime() {
            return closeTime;
        }

        public void dispose() throws IOException {
            wrappedChannel.close();
        }
    }


    private synchronized void pool(@NotNull final CachedChannel cachedChannel, @NotNull final ChannelType channelType) throws IOException {
        pools.get(channelType).pool(cachedChannel);
        if (poolSize() > maxSize) {
            releaseOther();
        }
    }

}
