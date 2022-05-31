package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.TstValues;
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
public class RowSequenceBench {
    private RowSet ix = null;
    private static final int chunkSz = 1024;
    private WritableLongChunk<OrderedRowKeys> indicesChunk = null;
    private WritableLongChunk<OrderedRowKeyRanges> rangesChunk = null;
    private static final int fixedCostChunkSz = 1024;
    private RowSequence fixedCostOk = null;

    @Setup(Level.Trial)
    public void setup() {
        indicesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
        rangesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
        final RowSetBuilderRandom ib = RowSetFactory.builderRandom();
        final TstValues.Builder tb = new TstValues.Builder() {
            @Override
            public void add(final long v) {
                ib.addKey(v);
            }

            @Override
            public void done() {
                ix = ib.build();
            }
        };
        TstValues.setup(tb, 16 * 1024 * 1024, TstValues.asymmetric);
        final WritableLongChunk<OrderedRowKeys> fixedCostChunk =
                WritableLongChunk.makeWritableChunk(fixedCostChunkSz);
        final Random r = new Random(1);
        long last = 0;
        for (int i = 0; i < fixedCostChunkSz; ++i) {
            last += r.nextInt(1009);
            fixedCostChunk.set(i, last);
        }
        fixedCostOk = RowSequenceFactory.takeRowKeysChunkAndMakeRowSequence(fixedCostChunk);
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
        fixedCostOk.forEachRowKey((final long v) -> {
            accum.setValue(accum.longValue() ^ v);
            return true;
        });
        indicesChunk.setSize(chunkSz);
        fixedCostOk.fillRowKeyChunk(indicesChunk);
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
        final RowSet.Iterator it = ix.iterator();
        while (it.hasNext()) {
            updateCrc32(crc32, it.nextLong());
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b02_IndexForEach(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        ix.forEachRowKey((final long v) -> {
            updateCrc32(crc32, v);
            return true;
        });
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b03_rsIteratorThenFillChunk(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        indicesChunk.setSize(chunkSz);
        while (rsIt.hasMore()) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSz);
            rs.fillRowKeyChunk(indicesChunk);
            for (int i = 0; i < indicesChunk.size(); ++i) {
                updateCrc32(crc32, indicesChunk.get(i));
            }
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b04_rsIteratorThenForEach(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        indicesChunk.setSize(chunkSz);
        while (rsIt.hasMore()) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSz);
            rs.forEachRowKey((final long v) -> {
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
        final RowSet.RangeIterator it = ix.rangeIterator();
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
        ix.forAllRowKeyRanges((final long s, final long e) -> {
            for (long v = s; v <= e; ++v) {
                updateCrc32(crc32, v);
            }
        });
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    @Benchmark
    public void b07_rsIteratorThenFillRangeChunk(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        while (rsIt.hasMore()) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSz / 2);
            rangesChunk.setSize(chunkSz);
            rs.fillRowKeyRangesChunk(rangesChunk);
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
    public void b08_rsIteratorThenForEachRange(final Blackhole bh) {
        final CRC32 crc32 = new CRC32();
        final RowSequence.Iterator rsIt = ix.getRowSequenceIterator();
        while (rsIt.hasMore()) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSz / 2);
            rs.forAllRowKeyRanges((final long s, final long e) -> {
                for (long v = s; v <= e; ++v) {
                    updateCrc32(crc32, v);
                }
            });
        }
        bh.consume(crc32.getValue());
        bh.consume(fixedCost());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSequenceBench.class);
    }
}
