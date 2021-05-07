package io.deephaven.parquet.util;


import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedChannelProviderTest {

    private List<String> closed = new ArrayList<>();


    @org.junit.After
    public void tearDown() {
        closed.clear();
    }

    @Test
    public void testSimpleRead() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getReadChannel("r" + i);
            rc.close();
        }
        Assert.assertEquals(closed.size(), 900);
        for (int i = 0; i < 900; i++) {
            Assert.assertTrue(closed.get(i).endsWith("r" + (i)));
        }
    }

    @Test
    public void testSimpleReadWrite() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = ((i / 100) % 2 == 0 ? cachedChannelProvider.getReadChannel("r" + i) : cachedChannelProvider.getWriteChannel("w" + i,false));
            rc.close();
        }
        Assert.assertEquals(closed.size(), 900);
        Assert.assertTrue(closed.get(0).endsWith("r0"));
    }

    @Test
    public void testSimpleWrite() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getWriteChannel("w" + i, false);
            rc.close();
        }
        Assert.assertEquals(closed.size(), 900);
        for (int i = 0; i < 900; i++) {
            Assert.assertTrue(closed.get(i).endsWith("w" + (i)));
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
        Assert.assertEquals(closed.size(), 900);
        for (int i = 0; i < 900; i++) {
            Assert.assertTrue(closed.get(i).endsWith("a" + (i)));
        }
    }

    @Test
    public void testCloseOrder() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 20; i++) {
            List<SeekableByteChannel> channels = new ArrayList<>();
            for (int j = 0; j < 50; j++) {
                channels.add(cachedChannelProvider.getReadChannel("r" + (j + 50 * i)));
            }
            for (int j = 0; j < 50; j++) {
                channels.get(49 - j).close();
            }
        }
        Assert.assertEquals(closed.size(), 900);
        for (int i = 0; i < 1; i++) {
            List<SeekableByteChannel> channels = new ArrayList<>();
            for (int j = 0; j < 50; j++) {
                Assert.assertTrue(closed.get(j + 50 * i).endsWith("r" + (50 * i + 49 - j)));
            }
        }
    }

    @Test
    public void testReuse() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        SeekableByteChannel someResult[] = new SeekableByteChannel[50];
        for (int i = 0; i < 50; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getReadChannel("r" + i % 50);
            rc.close();
            someResult[i] = rc;
        }
        for (int i = 50; i < 1000; i++) {
            SeekableByteChannel rc = cachedChannelProvider.getReadChannel("r" + i % 50);
            rc.close();
            Assert.assertSame(rc, someResult[i % 50]);
        }
        Assert.assertEquals(closed.size(), 0);
    }

    @Test
    public void testReuse10() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        SeekableByteChannel someResult[] = new SeekableByteChannel[100];
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                SeekableByteChannel rc = cachedChannelProvider.getReadChannel("r" + i % 10);
                someResult[i * 10 + j] = rc;
            }
            for (int j = 0; j < 10; j++) {
                someResult[i * 10 + 9 - j].close();
            }
        }
        for (int j = 0; j < 10; j++) {
            SeekableByteChannel reuse[] = new SeekableByteChannel[100];
            for (int i = 0; i < 100; i++) {
                SeekableByteChannel rc = cachedChannelProvider.getReadChannel("r" + (i / 10) % 10);
                Assert.assertSame(rc, someResult[i % 100]);
                reuse[i] = rc;
            }
            for (int i = 0; i < 100; i++) {
                reuse[99 - i].close();
            }
        }
        Assert.assertEquals(closed.size(), 0);
    }


    private class TestChannelProvider implements SeekableChannelsProvider {

        AtomicInteger count = new AtomicInteger(0);

        @Override
        public SeekableByteChannel getReadChannel(String path) throws IOException {
            return new TestMockChannel(count.getAndIncrement(), path);
        }

        @Override
        public SeekableByteChannel getReadChannel(Path path) throws IOException {
            return new TestMockChannel(count.getAndIncrement(), path.toString());
        }

        @Override
        public SeekableByteChannel getWriteChannel(String path, boolean append) throws IOException {
            return new TestMockChannel(count.getAndIncrement(), path);
        }

        @Override
        public SeekableByteChannel getWriteChannel(Path path, boolean append) throws IOException {
            return new TestMockChannel(count.getAndIncrement(), path.toString());
        }
    }

    private class TestMockChannel implements SeekableByteChannel {


        private final int id;
        private final String path;

        public TestMockChannel(int id, String path) {
            this.id = id;
            this.path = path;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return 0;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return 0;
        }

        @Override
        public long position() throws IOException {
            return 0;
        }

        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            return null;
        }

        @Override
        public long size() throws IOException {
            return 0;
        }

        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() throws IOException {
            closing(id, path);
        }
    }

    private void closing(int id, String path) {
        closed.add(path);
    }
}
