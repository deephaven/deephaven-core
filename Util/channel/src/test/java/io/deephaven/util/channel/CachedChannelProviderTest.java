//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedChannelProviderTest {

    private final List<String> closed = new ArrayList<>();

    @Test
    public void testSimpleRead() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = CachedChannelProvider.create(wrappedProvider, 100);
        for (int ii = 0; ii < 100; ++ii) {
            final SeekableByteChannel[] sameFile = new SeekableByteChannel[10];
            for (int jj = 0; jj < sameFile.length; ++jj) {
                sameFile[jj] = cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + ii);
            }
            final ByteBuffer buffer = ByteBuffer.allocate(1);
            for (int jj = 0; jj < 10; ++jj) {
                // Call read to hit the assertions inside the mock channel, which doesn't read anything
                sameFile[jj].read(buffer);
                assertEquals(buffer.remaining(), buffer.capacity());
                sameFile[jj].close();
            }
        }
        assertEquals(900, closed.size());
        for (int ii = 0; ii < 900; ++ii) {
            assertTrue(closed.get(ii).endsWith("r" + ii / 10));
        }
    }

    @Test
    public void testSimplePooledReadChannelClose() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = CachedChannelProvider.create(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + i);
            rc.close();
        }
        assertEquals(900, closed.size());
        assertTrue(closed.get(0).endsWith("r0"));
    }

    @Test
    public void testCloseOrder() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = CachedChannelProvider.create(wrappedProvider, 100);
        for (int i = 0; i < 20; i++) {
            List<SeekableByteChannel> channels = new ArrayList<>();
            for (int j = 0; j < 50; j++) {
                channels.add(cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + (j + 50 * i)));
            }
            for (int j = 0; j < 50; j++) {
                channels.get(49 - j).close();
            }
        }
        assertEquals(900, closed.size());
        for (int i = 0; i < 1; i++) {
            for (int j = 0; j < 50; j++) {
                assertTrue(closed.get(j + 50 * i).endsWith("r" + (50 * i + 49 - j)));
            }
        }
    }

    @Test
    public void testReuse() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = CachedChannelProvider.create(wrappedProvider, 50);
        final SeekableByteChannel[] someResult = new SeekableByteChannel[50];
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        for (int ci = 0; ci < someResult.length; ++ci) {
            someResult[ci] = cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + ci);
            // Call read to hit the assertions inside the mock channel, which doesn't read anything
            someResult[ci].read(buffer);
        }
        for (int ci = 0; ci < someResult.length; ++ci) {
            someResult[someResult.length - ci - 1].close();
        }
        for (int step = 0; step < 10; ++step) {
            for (int ci = 0; ci < someResult.length; ++ci) {
                assertSame(someResult[ci],
                        cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + ci));
                // Call read to hit the assertions inside the mock channel, which doesn't read anything
                someResult[ci].read(buffer);
            }
            for (int ci = 0; ci < someResult.length; ++ci) {
                someResult[someResult.length - ci - 1].close();
            }
        }
        assertEquals(0, closed.size());
    }

    @Test
    void testRewrapCachedChannelProvider() {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = CachedChannelProvider.create(wrappedProvider, 100);
        try {
            CachedChannelProvider.create(cachedChannelProvider, 100);
            fail("Expected IllegalArgumentException on rewrapping CachedChannelProvider");
        } catch (final IllegalArgumentException expected) {
        }
    }


    private class TestChannelProvider implements SeekableChannelsProvider {

        AtomicInteger count = new AtomicInteger(0);

        private final class TestChannelContext implements SeekableChannelContext {
            @Override
            @Nullable
            public <T extends SafeCloseable> T getCachedResource(final String key, final Supplier<T> resourceFactory) {
                throw new UnsupportedOperationException("getCachedResource");
            }
        }

        @Override
        public SeekableChannelContext makeContext() {
            return new TestChannelContext();
        }

        @Override
        public boolean isCompatibleWith(@NotNull SeekableChannelContext channelContext) {
            return channelContext == SeekableChannelContext.NULL;
        }

        @Override
        public boolean exists(@NotNull URI uri) {
            throw new UnsupportedOperationException("exists");
        }

        @Override
        public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext,
                @NotNull String path) {
            return new TestMockChannel(count.getAndIncrement(), path, channelContext);
        }

        @Override
        public InputStream getInputStream(SeekableByteChannel channel, int sizeHint) {
            // TestMockChannel is always empty, so no need to buffer
            return Channels.newInputStreamNoClose(channel);
        }

        @Override
        public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri) {
            return new TestMockChannel(count.getAndIncrement(), uri.toString(), channelContext);
        }

        @Override
        public CompletableOutputStream getOutputStream(@NotNull final URI uri, int bufferSizeHint) {
            throw new UnsupportedOperationException("getOutputStream");
        }

        @Override
        public final Stream<URI> list(@NotNull final URI directory) {
            throw new UnsupportedOperationException("list");
        }

        @Override
        public final Stream<URI> walk(@NotNull final URI directory) {
            throw new UnsupportedOperationException("walk");
        }

        @Override
        public void close() {}
    }

    private final class TestMockChannel implements SeekableByteChannel, CachedChannelProvider.ContextHolder {

        private final int id;
        private final String path;
        private SeekableChannelContext channelContext;

        private TestMockChannel(int id, String path, SeekableChannelContext channelContext) {
            this(id, path);
            this.channelContext = channelContext;
        }

        private TestMockChannel(int id, String path) {
            this.id = id;
            this.path = path;
            this.channelContext = null;
        }

        @Override
        public int read(ByteBuffer dst) {
            assertTrue(channelContext instanceof TestChannelProvider.TestChannelContext);
            return 0;
        }

        @Override
        public int write(ByteBuffer src) {
            assertNull(channelContext);
            return 0;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public SeekableByteChannel position(long newPosition) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public SeekableByteChannel truncate(long size) {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {
            closing(id, path);
            clearContext();
        }

        @Override
        public void setContext(@Nullable SeekableChannelContext channelContext) {
            this.channelContext = channelContext;
        }
    }

    private void closing(int id, String path) {
        closed.add(path);
    }
}
