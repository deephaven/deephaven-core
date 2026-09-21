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
 * {@link BarrageCopyKernel#copy}: one typed array copy per stretch, straight from the runs.
 *
 * <p>
 * Two more arms copy <em>two</em> columns of one type per operation, to show what columns sharing a
 * {@link BarrageCopyKernel.Runs} save each other, and report the time for both copies together:
 *
 * <ul>
 * <li>{@link #twoColumnsSeparateRuns} gives each column its own runs, as columns with different modification patterns
 * get.</li>
 * <li>{@link #twoColumnsSharedRuns} hands both columns the same runs, as columns with one modification pattern get. The
 * kernel only reads the runs, so this should cost exactly what separate runs cost; the pair is the control that sharing
 * is free.</li>
 * </ul>
 *
 * <p>
 * The runs are built once per trial. The kernel does not modify them, so nothing about one operation changes the next,
 * and rebuilding them per operation would only leave garbage for the collector to find inside the measurement: an
 * earlier version did that and it was the largest allocation in the benchmark by far.
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

    /**
     * The run lengths the published comparison reports, from a run per row up to runs long enough that one array copy
     * moves fifty of them.
     */
    @Param({"1", "2", "3", "5", "10", "50"})
    public int avgRunLength;

    private ChunkType chunkType;

    /** The runs as plain arrays, which {@link #mappingOnly} expands directly. */
    private long[] runDest;
    private long[] runEncoded;
    private long[] runLen;

    /** The same runs as the kernel takes them, one per column. */
    private BarrageCopyKernel.Runs runs;
    private BarrageCopyKernel.Runs runsB;

    private WritableChunk<Values>[][] addChunks;
    private WritableChunk<Values>[][] modChunks;
    private WritableChunk<Values>[] dest;
    private WritableChunk<Values>[][] addChunksB;
    private WritableChunk<Values>[][] modChunksB;
    private WritableChunk<Values>[] destB;

    private BarrageCopyKernel kernel;

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

        runs = buildRuns();
        runsB = buildRuns();

        kernel = BarrageCopyKernel.makeBarrageCopyKernel(chunkType);
    }

    private BarrageCopyKernel.Runs buildRuns() {
        final BarrageCopyKernel.Runs built = new BarrageCopyKernel.Runs();
        for (int ri = 0; ri < runLen.length; ++ri) {
            built.add(runDest[ri], runEncoded[ri], runLen[ri]);
        }
        return built;
    }

    /**
     * One column through the shipped kernel: runs in, one typed array copy per stretch. The kernel casts the chunk
     * arrays inside the call, as the gather this is compared against casts its own, so neither arm is charged for work
     * the other does outside the measurement.
     */
    @Benchmark
    public void rangeAware(final Blackhole blackhole) {
        kernel.copy(runs, dest, addChunks, modChunks, DELTA_CHUNK_SIZE);
        blackhole.consume(dest);
    }

    /** Two columns whose runs are their own, as columns with different modification patterns have. */
    @Benchmark
    public void twoColumnsSeparateRuns(final Blackhole blackhole) {
        kernel.copy(runs, dest, addChunks, modChunks, DELTA_CHUNK_SIZE);
        kernel.copy(runsB, destB, addChunksB, modChunksB, DELTA_CHUNK_SIZE);
        blackhole.consume(dest);
        blackhole.consume(destB);
    }

    /** Two columns sharing one set of runs, as columns with one modification pattern have. */
    @Benchmark
    public void twoColumnsSharedRuns(final Blackhole blackhole) {
        kernel.copy(runs, dest, addChunks, modChunks, DELTA_CHUNK_SIZE);
        kernel.copy(runs, destB, addChunksB, modChunksB, DELTA_CHUNK_SIZE);
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
        // The previous kernel cast every delta's chunk array for the column; that cost belongs to this path.
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
