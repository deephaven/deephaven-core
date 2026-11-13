//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.container.ArrayContainer;
import io.deephaven.engine.rowset.impl.rsp.container.Container;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An array container filled by OR-ing in a few values at a time, as a row set accumulating many small row sets does
 * block by block.
 *
 * <p>
 * Each block receives {@code finalKeys} distinct values, uniformly random over the block so they form about as many
 * runs as values and the block stays an array container, dealt out in batches of {@code batchKeys} sorted values; the
 * last batch holds whatever is left. A batch lands anywhere in the block, not just past its end. {@link #containerIor}
 * ORs the batches for one block into a single container through {@link Container#ior}, so it measures how that method
 * sizes the array it grows into. {@link #rspInsert} ORs {@code blocks} blocks' worth of batches at a time into an
 * {@link RspBitmap}, the path the engine takes, which also optimizes each container after the OR and may trim the room
 * the OR left for growth.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1)
public class ArrayContainerIorBench {

    private static final long RANDOM_SEED = 1;
    private static final int BLOCK_SIZE = 1 << 16;

    /**
     * Values added to each block per OR. At least three, and leaving a remainder of at least three, so that each batch
     * is itself an array container.
     */
    @Param({"4", "16", "64"})
    private int batchKeys;

    /** Values in each block once every batch is in; below the 4096 at which a block becomes a bitmap. */
    @Param({"1000", "4000"})
    private int finalKeys;

    /** Blocks each {@link #rspInsert} batch spans. */
    @Param({"16"})
    private int blocks;

    private ArrayContainer[] batches;
    private RspBitmap[] rspBatches;

    @Setup
    public void setup() {
        final Random random = new Random(RANDOM_SEED);
        final int nBatches = (finalKeys + batchKeys - 1) / batchKeys;
        // [block][batch] -> sorted values
        final short[][][] values = new short[blocks][][];
        for (int block = 0; block < blocks; ++block) {
            values[block] = dealBatches(random, nBatches);
        }

        batches = new ArrayContainer[nBatches];
        for (int bb = 0; bb < nBatches; ++bb) {
            batches[bb] = ArrayContainer.makeByCopying(values[0][bb], 0, values[0][bb].length);
        }

        rspBatches = new RspBitmap[nBatches];
        for (int bb = 0; bb < nBatches; ++bb) {
            RspBitmap rb = new RspBitmap();
            for (int block = 0; block < blocks; ++block) {
                final long blockKey = (long) block * BLOCK_SIZE;
                for (final short v : values[block][bb]) {
                    rb = rb.appendUnsafe(blockKey + (v & 0xFFFF));
                }
            }
            rb.finishMutations();
            rspBatches[bb] = rb;
        }

        final long containerCard = containerIor();
        final long rspCard = rspInsert();
        if (containerCard != finalKeys || rspCard != (long) finalKeys * blocks) {
            throw new IllegalStateException("containerCard=" + containerCard + ", rspCard=" + rspCard);
        }
    }

    /**
     * {@code finalKeys} distinct values dealt into {@code nBatches} sorted batches of {@code batchKeys}, the last
     * holding the remainder.
     */
    private short[][] dealBatches(final Random random, final int nBatches) {
        final int[] all = new int[BLOCK_SIZE];
        for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
            all[ii] = ii;
        }
        for (int ii = 0; ii < finalKeys; ++ii) {
            final int jj = ii + random.nextInt(BLOCK_SIZE - ii);
            final int tmp = all[ii];
            all[ii] = all[jj];
            all[jj] = tmp;
        }
        final short[][] out = new short[nBatches][];
        for (int bb = 0; bb < nBatches; ++bb) {
            final int[] batch = Arrays.copyOfRange(all, bb * batchKeys, Math.min(finalKeys, (bb + 1) * batchKeys));
            Arrays.sort(batch);
            out[bb] = new short[batch.length];
            for (int ii = 0; ii < batch.length; ++ii) {
                out[bb][ii] = (short) batch[ii];
            }
        }
        return out;
    }

    @Benchmark
    public long containerIor() {
        Container c = new ArrayContainer();
        for (final ArrayContainer batch : batches) {
            c = c.ior(batch);
        }
        return c.getCardinality();
    }

    @Benchmark
    public long rspInsert() {
        RspBitmap target = new RspBitmap();
        for (final RspBitmap batch : rspBatches) {
            target = target.ixInsert(batch);
        }
        return target.getCardinality();
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(ArrayContainerIorBench.class);
    }
}
