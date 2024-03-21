//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedChannelProviderTest {

    private final List<String> closed = new ArrayList<>();

    @Test
    public void testSimpleRead() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
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
    public void testSimpleReadWrite() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc =
                    ((i / 100) % 2 == 0 ? cachedChannelProvider.getReadChannel(wrappedProvider.makeContext(), "r" + i)
                            : cachedChannelProvider.getWriteChannel("w" + i, false));
            rc.close();
        }
        assertEquals(900, closed.size());
        assertTrue(closed.get(0).endsWith("r0"));
    }

    @Test
    public void testSimpleWrite() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getWriteChannel("w" + i, false);
            // Call write to hit the assertions inside the mock channel
            final ByteBuffer buffer = ByteBuffer.allocate(1);
            rc.write(buffer);
            rc.close();
        }
        assertEquals(900, closed.size());
        for (int i = 0; i < 900; i++) {
            assertTrue(closed.get(i).endsWith("w" + (i)));
        }
    }

    @Test
    public void testSimpleAppend() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getWriteChannel("a" + i, true);
            rc.close();
        }
        assertEquals(900, closed.size());
        for (int i = 0; i < 900; i++) {
            assertTrue(closed.get(i).endsWith("a" + (i)));
        }
    }

    @Test
    public void testCloseOrder() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
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
        final CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 50);
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
    public void testReuse10() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        final SeekableByteChannel[] someResult = new SeekableByteChannel[100];
        for (int pi = 0; pi < 10; ++pi) {
            for (int ci = 0; ci < 10; ++ci) {
                someResult[pi * 10 + ci] = cachedChannelProvider.getWriteChannel("w" + pi % 10, false);
            }
            for (int ci = 0; ci < 10; ++ci) {
                someResult[pi * 10 + 9 - ci].close();
            }
        }
        for (int step = 0; step < 10; ++step) {
            final SeekableByteChannel[] reused = new SeekableByteChannel[100];
            for (int ri = 0; ri < 100; ++ri) {
                SeekableByteChannel rc = cachedChannelProvider.getWriteChannel("w" + (ri / 10) % 10, false);
                assertSame(rc, someResult[ri % 100]);
                reused[ri] = rc;
            }
            for (int ri = 0; ri < 100; ++ri) {
                reused[99 - ri].close();
            }
        }
        assertEquals(0, closed.size());
    }


    private class TestChannelProvider implements SeekableChannelsProvider {

        AtomicInteger count = new AtomicInteger(0);

        private final class TestChannelContext implements SeekableChannelContext {
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
        public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext,
                @NotNull String path) {
            return new TestMockChannel(count.getAndIncrement(), path, channelContext);
        }

        @Override
        public InputStream getInputStream(SeekableByteChannel channel) {
            // TestMockChannel is always empty, so no need to buffer
            return Channels.newInputStreamNoClose(channel);
        }

        @Override
        public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri) {
            return new TestMockChannel(count.getAndIncrement(), uri.toString(), channelContext);
        }

        @Override
        public SeekableByteChannel getWriteChannel(@NotNull String path, boolean append) {
            return new TestMockChannel(count.getAndIncrement(), path);
        }

        @Override
        public SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) {
            return new TestMockChannel(count.getAndIncrement(), path.toString());
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
