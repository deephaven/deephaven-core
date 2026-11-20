//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.sparse;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK_SIZE;

/**
 * The row keys that the sparse benchmarks place in each storage of {@link SparseStorage}.
 * <p>
 * Patterns, for {@link SparseStorageBenchmark}, {@link SparseStorageMemory} and {@link SparseUpdateBenchmark}. Each
 * spans {@link #keySpace(String)} row keys, at least {@link #MIN_KEY_SPACE} and enough for about {@link #MIN_ROWS}
 * rows, so that every measurement covers at least that many rows:
 * <ul>
 * <li>Dense: every row key</li>
 * <li>Stride8, Stride64, Stride512: every 8th, 64th or 512th row key</li>
 * <li>Runs64: the first 64 row keys of every 512</li>
 * <li>Random10, Random1: each row key with probability 10% or 1%</li>
 * <li>Clustered: an eighth of the leaves full, and 4 random row keys in each other leaf</li>
 * </ul>
 * Which row keys a random pattern holds depends only on the row key, so that any range of the pattern's row keys is the
 * same whether it is built alone or as part of a larger range.
 * <p>
 * Shapes of a few rows, for {@link SparseSmallMemory} and {@link SparseSmallBenchmark}, each placing the rows where
 * they exercise a different part of the page table:
 * <ul>
 * <li>Dense: row keys 0 through n - 1</li>
 * <li>Stride64: every 64th row key from 0</li>
 * <li>High: n consecutive row keys from 3 * 2^23, in the upper quarter of a bottom page table level</li>
 * <li>Spread: every 8192nd row key from 0, one leaf in every eight</li>
 * <li>Level2s: every 2^25th row key from 0, one in each bottom page table level</li>
 * <li>Regions: n / 8 consecutive row keys at the start of each of eight regions 2^43 row keys apart, as a regioned
 * source numbers its rows</li>
 * <li>FarRegions: n / 2 consecutive row keys at the start of region 0 and of region 100000, so that the top page table
 * level's children are far apart</li>
 * </ul>
 */
final class SparsePatterns {
    /** The fewest rows a pattern holds, about. */
    static final int MIN_ROWS = 1 << 20;
    /** The fewest row keys a pattern spans. */
    static final long MIN_KEY_SPACE = 1L << 23;

    /** The number of random row keys in each leaf of Clustered that is not full. */
    private static final int CLUSTERED_KEYS = 4;

    private SparsePatterns() {}

    /**
     * @return the mean number of row keys {@code pattern} spans for each row it holds, rounded to an integer
     */
    static int keysPerRow(final String pattern) {
        switch (pattern) {
            case "Dense":
                return 1;
            case "Stride8":
            case "Runs64":
            case "Clustered":
                return 8;
            case "Stride64":
                return 64;
            case "Stride512":
                return 512;
            case "Random10":
                return 10;
            case "Random1":
                return 100;
            default:
                throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
    }

    /**
     * @return the number of row keys {@code pattern} spans from 0: at least {@link #MIN_KEY_SPACE}, and enough for
     *         about {@link #MIN_ROWS} rows; a multiple of the block size
     */
    static long keySpace(final String pattern) {
        return Math.max(MIN_KEY_SPACE, (long) MIN_ROWS * keysPerRow(pattern));
    }

    /**
     * @return the row keys of {@code pattern} over its whole {@link #keySpace(String) key space}
     */
    static WritableRowSet keys(final String pattern) {
        return keys(pattern, 0, keySpace(pattern));
    }

    /**
     * @return the row keys of {@code pattern} from {@code start} (inclusive) to {@code end} (exclusive), both multiples
     *         of the block size
     */
    static WritableRowSet keys(final String pattern, final long start, final long end) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        switch (pattern) {
            case "Dense":
                return RowSetFactory.fromRange(start, end - 1);
            case "Stride8":
                return stride(start, end, 8);
            case "Stride64":
                return stride(start, end, 64);
            case "Stride512":
                return stride(start, end, 512);
            case "Runs64":
                for (long key = start; key < end; key += 512) {
                    builder.appendRange(key, key + 63);
                }
                return builder.build();
            case "Random10":
                return random(start, end, 10);
            case "Random1":
                return random(start, end, 100);
            case "Clustered": {
                final long[] offsets = new long[CLUSTERED_KEYS];
                for (long leafStart = start; leafStart < end; leafStart += BLOCK_SIZE) {
                    if (Long.remainderUnsigned(mix(leafStart), 8) == 0) {
                        builder.appendRange(leafStart, leafStart + BLOCK_SIZE - 1);
                        continue;
                    }
                    // distinct offsets, drawn from the hashes of the row keys that follow the leaf's first
                    int found = 0;
                    for (long draw = leafStart + 1; found < CLUSTERED_KEYS; ++draw) {
                        final long offset = mix(draw) & (BLOCK_SIZE - 1);
                        boolean repeated = false;
                        for (int oi = 0; oi < found; ++oi) {
                            repeated |= offsets[oi] == offset;
                        }
                        if (!repeated) {
                            offsets[found++] = offset;
                        }
                    }
                    Arrays.sort(offsets);
                    for (final long offset : offsets) {
                        builder.appendKey(leafStart + offset);
                    }
                }
                return builder.build();
            }
            default:
                throw new IllegalArgumentException("Unknown pattern " + pattern);
        }
    }

    /**
     * @return the row keys of {@code shape} for a result of {@code size} rows
     */
    static RowSet smallKeys(final String shape, final int size) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        switch (shape) {
            case "Dense":
                return RowSetFactory.flat(size);
            case "Stride64":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii * 64);
                }
                return builder.build();
            case "Spread":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii * 8192);
                }
                return builder.build();
            case "High":
                return RowSetFactory.fromRange(3L << 23, (3L << 23) + size - 1);
            case "Level2s":
                for (long ii = 0; ii < size; ++ii) {
                    builder.appendKey(ii << 25);
                }
                return builder.build();
            case "FarRegions":
                builder.appendRange(0, (size + 1) / 2 - 1);
                if (size / 2 > 0) {
                    builder.appendRange(100_000L << 43, (100_000L << 43) + size / 2 - 1);
                }
                return builder.build();
            case "Regions":
                for (long region = 0; region < 8; ++region) {
                    final long first = region << 43;
                    final long count = size / 8 + (region < size % 8 ? 1 : 0);
                    if (count > 0) {
                        builder.appendRange(first, first + count - 1);
                    }
                }
                return builder.build();
            default:
                throw new IllegalArgumentException("Unknown shape " + shape);
        }
    }

    private static WritableRowSet stride(final long start, final long end, final int stride) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = start; key < end; key += stride) {
            builder.appendKey(key);
        }
        return builder.build();
    }

    /**
     * @return each row key from {@code start} to {@code end} whose hash is a multiple of {@code oneIn}
     */
    private static WritableRowSet random(final long start, final long end, final int oneIn) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long key = start; key < end; ++key) {
            if (Long.remainderUnsigned(mix(key), oneIn) == 0) {
                builder.appendKey(key);
            }
        }
        return builder.build();
    }

    private static long mix(final long key) {
        long value = key * 0x9E3779B97F4A7C15L;
        value ^= value >>> 32;
        value *= 0xD6E8FEB86659FD93L;
        return value ^ (value >>> 32);
    }
}
