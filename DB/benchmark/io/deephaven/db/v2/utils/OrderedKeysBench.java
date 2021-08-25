package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import org.apache.commons.lang3.mutable.MutableLong;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.zip.CRC32;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 12)
@Measurement(iterations = 3, time = 12)
@Fork(value = 1)
public class OrderedKeysBench {
    private Index ix = null;
    private static final int chunkSz = 1024;
    private WritableLongChunk<OrderedKeyIndices> indicesChunk = null;
    private WritableLongChunk<OrderedKeyRanges> rangesChunk = null;
    private static final int fixedCostChunkSz = 1024;
    private OrderedKeys fixedCostOk = null;

    @Setup(Level.Trial)
    public void setup() {
        indicesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
        rangesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
        final Index.RandomBuilder ib = Index.FACTORY.getRandomBuilder();
        final TestValues.Builder tb = new TestValues.Builder() {
            @Override
            public void add(final long v) {
                ib.addKey(v);
            }

            @Override
            public void done() {
                ix = ib.getIndex();
            }
        };
        TestValues.setup(tb, 16 * 1024 * 1024, TestValues.asymmetric);
        final WritableLongChunk<OrderedKeyIndices> fixedCostChunk =
            WritableLongChunk.makeWritableChunk(fixedCostChunkSz);
        final Random r = new Random(1);
        long last = 0;
        for (int i = 0; i < fixedCostChunkSz; ++i) {
            last += r.nextInt(1009);
            fixedCostChunk.set(i, last);
        }
        fixedCostOk = OrderedKeysKeyIndicesChunkImpl.makeByTaking(fixedCostChunk);
    }

    static void updateCrc32(final CRC32 crc32, final long v) {
        for (int bi = 0; bi < 8; ++bi) {
            final int b = (int) ((0x00000000000000FFL) & (v >> 8 * bi));
            crc32.update(b);
        }
    }

    // Ensure that our calls to forEach are not on a single visible type of OK for the JVM.
    long fixedCost() {
        final MutableLong accum = new MutableLong(0);
        fixedCostOk.forEachLong((final long v) -> {
            accum.setValue(accum.longValue() ^ v);
            return true;
        });
        indicesChunk.setSize(chunkSz);
        fixedCostOk.fillKeyIndicesChunk(indicesChunk);
        final int sz = indicesChunk.size();
        for (int i = 0; i < sz; ++i) {
            accum.setValue(accum.longValue() ^ indicesChunk.get(i));
        }
        return accum.getValue();
    }

    @Benchmark
    public void b00_FixedCostCost(final Blackhole bh) {
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b01_IndexIterator(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final Index.Iterator it = ix.iterator();
        while (it.hasNext()) {
            updateCrc32(crc32, it.nextLong());
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b02_IndexForEach(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        ix.forEachLong((final long v) -> {
            updateCrc32(crc32, v);
            return true;
        });
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b03_OKIteratorThenFillChunk(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        indicesChunk.setSize(chunkSz);
        while (okit.hasMore()) {
            final OrderedKeys oks = okit.getNextOrderedKeysWithLength(chunkSz);
            oks.fillKeyIndicesChunk(indicesChunk);
            for (int i = 0; i < indicesChunk.size(); ++i) {
                updateCrc32(crc32, indicesChunk.get(i));
            }
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b04_OKIteratorThenForEach(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        indicesChunk.setSize(chunkSz);
        while (okit.hasMore()) {
            final OrderedKeys oks = okit.getNextOrderedKeysWithLength(chunkSz);
            oks.forEachLong((final long v) -> {
                updateCrc32(crc32, v);
                return true;
            });
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b05_IndexRangeIterator(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final Index.RangeIterator it = ix.rangeIterator();
        while (it.hasNext()) {
            it.next();
            final long s = it.currentRangeStart();
            final long e = it.currentRangeEnd();
            for (long v = s; v <= e; ++v) {
                updateCrc32(crc32, v);
            }
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b06_IndexForEachRange(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        ix.forAllLongRanges((final long s, final long e) -> {
            for (long v = s; v <= e; ++v) {
                updateCrc32(crc32, v);
            }
        });
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b07_OKIteratorThenFillRangeChunk(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        while (okit.hasMore()) {
            final OrderedKeys oks = okit.getNextOrderedKeysWithLength(chunkSz / 2);
            rangesChunk.setSize(chunkSz);
            oks.fillKeyRangesChunk(rangesChunk);
            final int sz = rangesChunk.size();
            for (int i = 0; i < sz; i += 2) {
                final long start = rangesChunk.get(i);
                final long end = rangesChunk.get(i + 1);
                for (long v = start; v <= end; ++v) {
                    updateCrc32(crc32, v);
                }
            }
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b08_OKIteratorThenForEachRange(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final OrderedKeys.Iterator okit = ix.getOrderedKeysIterator();
        while (okit.hasMore()) {
            final OrderedKeys oks = okit.getNextOrderedKeysWithLength(chunkSz / 2);
            oks.forAllLongRanges((final long s, final long e) -> {
                for (long v = s; v <= e; ++v) {
                    updateCrc32(crc32, v);
                }
            });
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(OrderedKeysBench.class);
    }
}
