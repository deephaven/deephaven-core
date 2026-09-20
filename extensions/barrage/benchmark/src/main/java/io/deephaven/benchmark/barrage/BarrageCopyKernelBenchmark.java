//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.BarrageCopyKernel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Copying the columns of a coalesced Barrage update out of the chunks of the per-cycle updates they were built from.
 *
 * <p>
 * Two one-column arms compare the algorithms. {@link #mappingOnly} is what main does: expand the surviving rows into a
 * mapping holding one encoded origin per output row, then gather cell by cell from it, whatever the run structure; the
 * expansion is timed with the gather because main cannot copy without it. {@link #rangeAware} is
 * {@link BarrageCopyKernel#copy}, which chooses from the average run length between an array copy per stretch and an
 * element fill through the same kind of mapping.
 *
 * <p>
 * Two more arms copy <em>two</em> columns of one type per operation, to show what columns sharing a
 * {@link BarrageCopyKernel.Runs} save each other, and report the time for both copies together:
 *
 * <ul>
 * <li>{@link #twoColumnsSeparateRuns} gives each column its own runs, as columns with different modification patterns
 * get. Below the array-copy threshold each column builds its own mapping.</li>
 * <li>{@link #twoColumnsSharedRuns} hands both columns the same runs, as columns with one modification pattern get. At
 * or above the threshold neither column builds a mapping, so this should cost what separate runs cost; below it the
 * first column converts the runs and the second reads what it left, so the pair should pay for one mapping.</li>
 * </ul>
 *
 * <p>
 * The runs are rebuilt before every operation, outside the measurement, because converting a {@code Runs} consumes it:
 * without that, only the first operation of an iteration would pay for the mapping and the rest would read a cached
 * one.
 */
@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 4, time = 2)
public class BarrageCopyKernelBenchmark {

    /**
     * Matches the producer's delta chunk size by default, so boundary crossings happen as often as in production.
     * Overridable with {@code -Dbench.deltaChunkSize=} to check whether a smaller run says the same thing.
     */
    private static final int DELTA_CHUNK_SIZE = Integer.getInteger("bench.deltaChunkSize", 1 << 16);
    /**
     * Sixteen delta chunks, 2^20 rows, by default; run lengths that are not powers of two round the run count up.
     * Overridable with {@code -Dbench.totalRows=}.
     */
    private static final int TOTAL_ROWS = Integer.getInteger("bench.totalRows", 16 * DELTA_CHUNK_SIZE);
    private static final int NUM_DELTAS = 2;

    @Param({"Int", "Double", "String"})
    public String columnType;

    /** Lengths either side of the kernel's array-copy threshold, from a run per row upward. */
    @Param({"1", "2", "5", "16", "50"})
    public int avgRunLength;

    private ChunkType chunkType;

    /** The runs as plain arrays, from which a fresh {@link BarrageCopyKernel.Runs} is built for every operation. */
    private long[] runDest;
    private long[] runEncoded;
    private long[] runLen;

    /** Rebuilt per operation: one for each of the two columns, unconverted. */
    private BarrageCopyKernel.Runs runsA;
    private BarrageCopyKernel.Runs runsB;

    private WritableChunk<Values>[][] addChunks;
    private WritableChunk<Values>[][] modChunks;
    private WritableChunk<Values>[] dest;
    private WritableChunk<Values>[][] addChunksB;
    private WritableChunk<Values>[][] modChunksB;
    private WritableChunk<Values>[] destB;

    private BarrageCopyKernel kernel;
    private BarrageCopyKernel.BarrageCopyKernelContext context;
    private BarrageCopyKernel.BarrageCopyKernelContext contextB;

    private static Object value(final ChunkType chunkType, final int index) {
        switch (chunkType) {
            case Int:
                return index;
            case Double:
                return index * 0.5;
            default:
                return "v" + index;
        }
    }

    @SuppressWarnings("unchecked")
    private WritableChunk<Values>[] makeSide(final int seed) {
        final int numChunks = (TOTAL_ROWS + DELTA_CHUNK_SIZE - 1) / DELTA_CHUNK_SIZE;
        final WritableChunk<Values>[] chunks = new WritableChunk[numChunks];
        for (int ci = 0; ci < numChunks; ++ci) {
            final int rows = Math.min(DELTA_CHUNK_SIZE, TOTAL_ROWS - ci * DELTA_CHUNK_SIZE);
            final WritableChunk<Values> chunk = chunkType.makeWritableChunk(rows);
            for (int ii = 0; ii < rows; ++ii) {
                final int index = seed + ci * DELTA_CHUNK_SIZE + ii;
                switch (chunkType) {
                    case Int:
                        chunk.asWritableIntChunk().set(ii, (Integer) value(chunkType, index));
                        break;
                    case Double:
                        chunk.asWritableDoubleChunk().set(ii, (Double) value(chunkType, index));
                        break;
                    default:
                        chunk.<Object>asWritableObjectChunk().set(ii, value(chunkType, index));
                        break;
                }
            }
            chunks[ci] = chunk;
        }
        return chunks;
    }

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        chunkType = "Int".equals(columnType) ? ChunkType.Int
                : "Double".equals(columnType) ? ChunkType.Double
                        : ChunkType.Object;

        addChunks = new WritableChunk[NUM_DELTAS][];
        modChunks = new WritableChunk[NUM_DELTAS][];
        addChunksB = new WritableChunk[NUM_DELTAS][];
        modChunksB = new WritableChunk[NUM_DELTAS][];
        for (int di = 0; di < NUM_DELTAS; ++di) {
            addChunks[di] = makeSide(di * 1_000_000);
            modChunks[di] = makeSide(500_000 + di * 1_000_000);
            addChunksB[di] = makeSide(3_000_000 + di * 1_000_000);
            modChunksB[di] = makeSide(3_500_000 + di * 1_000_000);
        }

        final int numChunks = (TOTAL_ROWS + DELTA_CHUNK_SIZE - 1) / DELTA_CHUNK_SIZE;
        dest = new WritableChunk[numChunks];
        destB = new WritableChunk[numChunks];
        for (int ci = 0; ci < numChunks; ++ci) {
            final int rows = Math.min(DELTA_CHUNK_SIZE, TOTAL_ROWS - ci * DELTA_CHUNK_SIZE);
            dest[ci] = chunkType.makeWritableChunk(rows);
            destB[ci] = chunkType.makeWritableChunk(rows);
        }

        // Runs of the requested length, taking their origins from alternating deltas and sides at scattered positions,
        // so neither the origin chunk nor the offset within it is predictable from the output position.
        final int numRuns = (TOTAL_ROWS + avgRunLength - 1) / avgRunLength;
        runDest = new long[numRuns];
        runEncoded = new long[numRuns];
        runLen = new long[numRuns];
        final Random random = new Random(0xB0A7L);
        long destPos = 0;
        int ri = 0;
        while (destPos < TOTAL_ROWS) {
            final int length = (int) Math.min(avgRunLength, TOTAL_ROWS - destPos);
            final long encoded = BarrageCopyKernel.originOffset(random.nextInt(NUM_DELTAS), random.nextBoolean())
                    + random.nextInt(TOTAL_ROWS - length + 1);
            runDest[ri] = destPos;
            runEncoded[ri] = encoded;
            runLen[ri] = length;
            ++ri;
            destPos += length;
        }

        kernel = BarrageCopyKernel.makeBarrageCopyKernel(chunkType);
        context = kernel.makeContext(addChunks, modChunks, DELTA_CHUNK_SIZE);
        contextB = kernel.makeContext(addChunksB, modChunksB, DELTA_CHUNK_SIZE);
    }

    /**
     * Fresh runs for every operation. Converting a {@code Runs} to its mapping drops the runs, so a converted one
     * cannot be measured twice; building them here keeps that cost out of the measurement.
     */
    @Setup(Level.Invocation)
    public void newRuns() {
        runsA = buildRuns();
        runsB = buildRuns();
    }

    private BarrageCopyKernel.Runs buildRuns() {
        final BarrageCopyKernel.Runs built = new BarrageCopyKernel.Runs();
        for (int ri = 0; ri < runLen.length; ++ri) {
            built.add(runDest[ri], runEncoded[ri], runLen[ri]);
        }
        return built;
    }

    /** One column through the shipped kernel: runs in, the kernel picks how to move them. */
    @Benchmark
    public void rangeAware(final Blackhole blackhole) {
        kernel.copy(runsA, dest, context);
        blackhole.consume(dest);
    }

    /**
     * Two columns whose runs are their own, as columns with different modification patterns have. Below the array-copy
     * threshold each column expands its own mapping.
     */
    @Benchmark
    public void twoColumnsSeparateRuns(final Blackhole blackhole) {
        kernel.copy(runsA, dest, context);
        kernel.copy(runsB, destB, contextB);
        blackhole.consume(dest);
        blackhole.consume(destB);
    }

    /**
     * Two columns sharing one set of runs, as columns with one modification pattern have. Below the array-copy
     * threshold the first column converts the runs and the second copies from the mapping it left.
     */
    @Benchmark
    public void twoColumnsSharedRuns(final Blackhole blackhole) {
        kernel.copy(runsA, dest, context);
        kernel.copy(runsA, destB, contextB);
        blackhole.consume(dest);
        blackhole.consume(destB);
    }

    /**
     * Main's arrangement for one column: expand the surviving rows into a per-row mapping and have the typed kernel
     * gather from it. Timed whole, expansion included, because main builds the mapping on every propagation before it
     * can copy.
     */
    @Benchmark
    public void mappingOnly(final Blackhole blackhole) {
        mappingOnlyCopy();
        blackhole.consume(dest);
    }

    /** Main's copy: expand the rows to one encoded origin per output row, then gather cell by cell. */
    private void mappingOnlyCopy() {
        final long[][] mapping = new long[dest.length][];
        for (int mi = 0; mi < dest.length; ++mi) {
            mapping[mi] = new long[dest[mi].size()];
        }
        for (int ri = 0; ri < runLen.length; ++ri) {
            long destPos = runDest[ri];
            long origin = runEncoded[ri];
            for (long remaining = runLen[ri]; remaining > 0; --remaining) {
                mapping[(int) (destPos / DELTA_CHUNK_SIZE)][(int) (destPos % DELTA_CHUNK_SIZE)] = origin;
                ++destPos;
                ++origin;
            }
        }

        switch (chunkType) {
            case Int:
                gatherInt(mapping);
                break;
            case Double:
                gatherDouble(mapping);
                break;
            default:
                gatherObject(mapping);
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private void gatherInt(final long[][] mapping) {
        // The previous kernel's context cast every delta's chunk array for the column; that cost belongs to this path.
        final WritableIntChunk<Values>[][] adds = new WritableIntChunk[NUM_DELTAS][];
        final WritableIntChunk<Values>[][] mods = new WritableIntChunk[NUM_DELTAS][];
        for (int di = 0; di < NUM_DELTAS; ++di) {
            adds[di] = new WritableIntChunk[addChunks[di].length];
            mods[di] = new WritableIntChunk[modChunks[di].length];
            for (int ci = 0; ci < addChunks[di].length; ++ci) {
                adds[di][ci] = addChunks[di][ci].asWritableIntChunk();
                mods[di][ci] = modChunks[di][ci].asWritableIntChunk();
            }
        }
        for (int mi = 0; mi < dest.length; ++mi) {
            final long[] chunkMapping = mapping[mi];
            final WritableIntChunk<Values> destChunk = dest[mi].asWritableIntChunk();
            for (int pos = 0; pos < chunkMapping.length; ++pos) {
                final long encoded = chunkMapping[pos];
                final int deltaIdx =
                        (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
                final WritableIntChunk<Values>[] originChunks =
                        (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0 ? mods[deltaIdx] : adds[deltaIdx];
                final long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
                destChunk.set(pos, originChunks[(int) (originPos / DELTA_CHUNK_SIZE)]
                        .get((int) (originPos % DELTA_CHUNK_SIZE)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void gatherDouble(final long[][] mapping) {
        final WritableDoubleChunk<Values>[][] adds = new WritableDoubleChunk[NUM_DELTAS][];
        final WritableDoubleChunk<Values>[][] mods = new WritableDoubleChunk[NUM_DELTAS][];
        for (int di = 0; di < NUM_DELTAS; ++di) {
            adds[di] = new WritableDoubleChunk[addChunks[di].length];
            mods[di] = new WritableDoubleChunk[modChunks[di].length];
            for (int ci = 0; ci < addChunks[di].length; ++ci) {
                adds[di][ci] = addChunks[di][ci].asWritableDoubleChunk();
                mods[di][ci] = modChunks[di][ci].asWritableDoubleChunk();
            }
        }
        for (int mi = 0; mi < dest.length; ++mi) {
            final long[] chunkMapping = mapping[mi];
            final WritableDoubleChunk<Values> destChunk = dest[mi].asWritableDoubleChunk();
            for (int pos = 0; pos < chunkMapping.length; ++pos) {
                final long encoded = chunkMapping[pos];
                final int deltaIdx =
                        (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
                final WritableDoubleChunk<Values>[] originChunks =
                        (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0 ? mods[deltaIdx] : adds[deltaIdx];
                final long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
                destChunk.set(pos, originChunks[(int) (originPos / DELTA_CHUNK_SIZE)]
                        .get((int) (originPos % DELTA_CHUNK_SIZE)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void gatherObject(final long[][] mapping) {
        final WritableObjectChunk<Object, Values>[][] adds = new WritableObjectChunk[NUM_DELTAS][];
        final WritableObjectChunk<Object, Values>[][] mods = new WritableObjectChunk[NUM_DELTAS][];
        for (int di = 0; di < NUM_DELTAS; ++di) {
            adds[di] = new WritableObjectChunk[addChunks[di].length];
            mods[di] = new WritableObjectChunk[modChunks[di].length];
            for (int ci = 0; ci < addChunks[di].length; ++ci) {
                adds[di][ci] = addChunks[di][ci].asWritableObjectChunk();
                mods[di][ci] = modChunks[di][ci].asWritableObjectChunk();
            }
        }
        for (int mi = 0; mi < dest.length; ++mi) {
            final long[] chunkMapping = mapping[mi];
            final WritableObjectChunk<Object, Values> destChunk = dest[mi].asWritableObjectChunk();
            for (int pos = 0; pos < chunkMapping.length; ++pos) {
                final long encoded = chunkMapping[pos];
                final int deltaIdx =
                        (int) ((encoded >>> BarrageCopyKernel.DELTA_INDEX_SHIFT) & BarrageCopyKernel.DELTA_INDEX_MASK);
                final WritableObjectChunk<Object, Values>[] originChunks =
                        (encoded & (1L << BarrageCopyKernel.DELTA_MOD_FLAG_BIT)) != 0 ? mods[deltaIdx] : adds[deltaIdx];
                final long originPos = encoded & BarrageCopyKernel.DELTA_POSITION_MASK;
                destChunk.set(pos, originChunks[(int) (originPos / DELTA_CHUNK_SIZE)]
                        .get((int) (originPos % DELTA_CHUNK_SIZE)));
            }
        }
    }
}
