package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.PrimitiveIterator;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
public class IndexBuilderChunkedBench {
    private Index ix = null;
    private static final int chunkSz = 1024;
    private WritableLongChunk<OrderedKeyIndices> indicesChunk = null;
    private WritableLongChunk<OrderedKeyRanges> rangesChunk = null;

    @Setup(Level.Trial)
    public void setup() {
        indicesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
        rangesChunk = WritableLongChunk.makeWritableChunk(chunkSz);
    }

    private static final long maxOddValue = 8 * 1000 * 1000 + 1;

    @Benchmark
    public void buildOddIndexViaIndividualValues(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        for (long v = 1; v <= maxOddValue; v += 2) {
            b.appendKey(v);
        }
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    @Benchmark
    public void buildOddIndexViaChunks(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        int ci = 0;
        indicesChunk.setSize(chunkSz);
        for (long v = 1; v <= maxOddValue; v += 2) {
            indicesChunk.set(ci++, v);
            if (ci == chunkSz) {
                b.appendOrderedKeyIndicesChunk(indicesChunk);
                ci = 0;
            }
        }
        if (ci > 0) {
            indicesChunk.setSize(ci);
            b.appendOrderedKeyIndicesChunk(indicesChunk);
        }
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    @Benchmark
    public void buildOddIndexViaIterator(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        b.appendKeys(new PrimitiveIterator.OfLong() {
            long v = 1;

            @Override
            public boolean hasNext() {
                return v < maxOddValue;
            }

            @Override
            public long nextLong() {
                final long r = v;
                v += 2;
                return r;
            }
        });
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    @Benchmark
    public void buildOddIndexViaRanges(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        for (long v = 1; v < maxOddValue; v += 4) {
            b.appendRange(v, v + 2);
        }
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    @Benchmark
    public void buildOddIndexViaRangeChunks(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        int ci = 0;
        rangesChunk.setSize(chunkSz);
        for (long v = 1; v < maxOddValue; v += 4) {
            rangesChunk.set(ci++, v);
            rangesChunk.set(ci++, v + 2);
            if (ci == chunkSz) {
                b.appendOrderedKeyRangesChunk(rangesChunk);
                ci = 0;
            }
        }
        if (ci > 0) {
            rangesChunk.setSize(ci);
            b.appendOrderedKeyRangesChunk(rangesChunk);
        }
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    @Benchmark
    public void buildOddIndexViaRangeIterator(final Blackhole bh) {
        final Index.SequentialBuilder b = Index.FACTORY.getSequentialBuilder();
        b.appendRanges(new LongRangeIterator() {
            long v = -3;

            @Override
            public boolean hasNext() {
                return v < maxOddValue;
            }

            @Override
            public void next() {
                v += 4;
            }

            @Override
            public long start() {
                return v;
            }

            @Override
            public long end() {
                return v + 2;
            }
        });
        final Index ix = b.getIndex();
        bh.consume(ix);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(IndexBuilderChunkedBench.class);
    }
}
