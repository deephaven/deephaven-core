/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base.util;

import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.engine.util.file.FileHandleAccessor;
import io.deephaven.engine.util.file.FileHandleFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedChannelProviderTest {

    private final List<String> closed = new ArrayList<>();

    private final File f = File.createTempFile("TestFileHandle-", ".dat");

    private final FileHandle fh = new FileHandle(FileChannel.open(f.toPath(),
            StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE),
            () -> {
            });

    private final FileHandleFactory.FileToHandleFunction fthf = (final File file) -> fh;

    public CachedChannelProviderTest() throws IOException {}

    @org.junit.After
    public void tearDown() {
        closed.clear();
    }

    @Test
    public void testSimpleRead() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int ii = 0; ii < 100; ++ii) {
            final SeekableByteChannel[] sameFile = new SeekableByteChannel[10];
            for (int jj = 0; jj < sameFile.length; ++jj) {
                sameFile[jj] = cachedChannelProvider.getReadChannel("r" + ii);
            }
            for (int jj = 0; jj < 10; ++jj) {
                sameFile[jj].close();
            }
        }
        Assert.assertEquals(closed.size(), 900);
        for (int ii = 0; ii < 900; ++ii) {
            Assert.assertTrue(closed.get(ii).endsWith("r" + ii / 10));
        }
    }

    @Test
    public void testSimpleReadWrite() throws IOException {
        SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 100);
        for (int i = 0; i < 1000; i++) {
            SeekableByteChannel rc = ((i / 100) % 2 == 0 ? cachedChannelProvider.getReadChannel("r" + i)
                    : cachedChannelProvider.getWriteChannel("w" + i, false));
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
            for (int j = 0; j < 50; j++) {
                Assert.assertTrue(closed.get(j + 50 * i).endsWith("r" + (50 * i + 49 - j)));
            }
        }
    }

    @Test
    public void testReuse() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider cachedChannelProvider = new CachedChannelProvider(wrappedProvider, 50);
        final SeekableByteChannel[] someResult = new SeekableByteChannel[50];
        for (int ci = 0; ci < someResult.length; ++ci) {
            someResult[ci] = cachedChannelProvider.getReadChannel("r" + ci);
        }
        for (int ci = 0; ci < someResult.length; ++ci) {
            someResult[someResult.length - ci - 1].close();
        }
        for (int step = 0; step < 10; ++step) {
            for (int ci = 0; ci < someResult.length; ++ci) {
                Assert.assertSame(someResult[ci], cachedChannelProvider.getReadChannel("r" + ci));
            }
            for (int ci = 0; ci < someResult.length; ++ci) {
                someResult[someResult.length - ci - 1].close();
            }
        }
        Assert.assertEquals(closed.size(), 0);
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
                Assert.assertSame(rc, someResult[ri % 100]);
                reused[ri] = rc;
            }
            for (int ri = 0; ri < 100; ++ri) {
                reused[99 - ri].close();
            }
        }
        Assert.assertEquals(closed.size(), 0);
    }

    @Test
    public void testInvalidation() throws IOException {
        final SeekableChannelsProvider wrappedProvider = new TestChannelProvider();
        final CachedChannelProvider firstCCP, secondCCP, thirdCCP;
        SeekableByteChannel rc1, rc2, wc1;

        firstCCP = new CachedChannelProvider(wrappedProvider, 100);
        rc1 = firstCCP.getReadChannel("rc1");
        wc1 = firstCCP.getWriteChannel("wc1", false);
        // firstCCP -> rc1, wc1

        secondCCP = new CachedChannelProvider(wrappedProvider, 100);
        rc2 = secondCCP.getReadChannel("rc2");
        // secondCCP -> rc2

        CachedChannelProviderTracker.getInstance().invalidateChannels(new File("rc2"));
        Assert.assertTrue(!firstCCP.invalid() &&
                !((CachedChannelProvider.CachedChannel) rc1).invalid() &&
                !((CachedChannelProvider.CachedChannel) wc1).invalid());
        Assert.assertTrue(secondCCP.invalid() &&
                ((CachedChannelProvider.CachedChannel) rc2).invalid());

        thirdCCP = new CachedChannelProvider(wrappedProvider, 100);
        wc1 = thirdCCP.getWriteChannel("wc1", true);
        // thirdCCP -> wc1

        CachedChannelProviderTracker.getInstance().invalidateChannels(new File("wc1"));
        // Both firstCCP and thirdCCP, and their corresponding channels should be invalidated
        Assert.assertTrue(firstCCP.invalid() &&
                ((CachedChannelProvider.CachedChannel) rc1).invalid() &&
                ((CachedChannelProvider.CachedChannel) wc1).invalid());
        Assert.assertTrue(thirdCCP.invalid());
    }

    private class TestChannelProvider implements SeekableChannelsProvider {

        AtomicInteger count = new AtomicInteger(0);

        @Override
        public SeekableByteChannel getReadChannel(@NotNull String path) {
            return new TestMockChannel(count.getAndIncrement(), path);
        }

        @Override
        public SeekableByteChannel getReadChannel(@NotNull Path path) {
            return new TestMockChannel(count.getAndIncrement(), path.toString());
        }

        @Override
        public SeekableByteChannel getWriteChannel(@NotNull String path, boolean append) {
            return new TestMockChannel(count.getAndIncrement(), path);
        }

        @Override
        public SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) {
            return new TestMockChannel(count.getAndIncrement(), path.toString());
        }
    }

    private class TestMockChannel extends FileHandleAccessor implements SeekableByteChannel {
        private final String path;

        public TestMockChannel(int id, String path) {
            super(fthf, new File(path));
            this.path = path;
        }

        @Override
        public int read(ByteBuffer dst) {
            return 0;
        }

        @Override
        public int write(ByteBuffer src) {
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
            closing(path);
        }
    }

    private void closing(String path) {
        closed.add(path);
    }
}
