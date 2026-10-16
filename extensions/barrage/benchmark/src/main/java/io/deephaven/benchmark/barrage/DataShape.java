//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

import java.time.Instant;
import java.util.Random;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * The synthetic tables the compression benchmarks serialize. Compression ratios depend almost entirely on the data, so
 * each shape isolates one kind of column, and {@link #MIXED} approximates a realistic trades table. Every table is
 * generated from a fixed seed so runs are comparable.
 */
public enum DataShape {
    /** Uniformly random doubles; close to incompressible. */
    RANDOM_DOUBLE {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final double[][] columns = new double[4][numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                for (final double[] column : columns) {
                    column[ii] = random.nextDouble();
                }
            }
            return TableTools.newTable(
                    TableTools.doubleCol("D0", columns[0]),
                    TableTools.doubleCol("D1", columns[1]),
                    TableTools.doubleCol("D2", columns[2]),
                    TableTools.doubleCol("D3", columns[3]));
        }
    },

    /** Monotonic and run-length-friendly longs, such as ids, timestamps and bucket keys. */
    SEQUENTIAL_LONG {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final long[] id = new long[numRows];
            final long[] nanos = new long[numRows];
            final long[] bucket = new long[numRows];
            final long[] stride = new long[numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                id[ii] = ii;
                nanos[ii] = BASE_NANOS + ii * 1_000_000L;
                bucket[ii] = ii / 64;
                stride[ii] = ii * 17L;
            }
            return TableTools.newTable(
                    TableTools.longCol("Id", id),
                    TableTools.longCol("Nanos", nanos),
                    TableTools.longCol("Bucket", bucket),
                    TableTools.longCol("Stride", stride));
        }
    },

    /** Strings drawn from a 32-symbol vocabulary. */
    LOW_CARDINALITY_STRING {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final String[][] columns = new String[4][numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                for (final String[] column : columns) {
                    column[ii] = SYMBOLS[random.nextInt(SYMBOLS.length)];
                }
            }
            return TableTools.newTable(
                    TableTools.stringCol("S0", columns[0]),
                    TableTools.stringCol("S1", columns[1]),
                    TableTools.stringCol("S2", columns[2]),
                    TableTools.stringCol("S3", columns[3]));
        }
    },

    /** Random 16-character hex strings; nearly every value is unique. */
    HIGH_CARDINALITY_STRING {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final String[][] columns = new String[4][numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                for (final String[] column : columns) {
                    column[ii] = randomHex(random);
                }
            }
            return TableTools.newTable(
                    TableTools.stringCol("H0", columns[0]),
                    TableTools.stringCol("H1", columns[1]),
                    TableTools.stringCol("H2", columns[2]),
                    TableTools.stringCol("H3", columns[3]));
        }
    },

    /** Random ints where 90% of the values are null. */
    NULL_HEAVY_INT {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final int[][] columns = new int[4][numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                for (final int[] column : columns) {
                    column[ii] = random.nextInt(10) == 0 ? random.nextInt() : NULL_INT;
                }
            }
            return TableTools.newTable(
                    TableTools.intCol("N0", columns[0]),
                    TableTools.intCol("N1", columns[1]),
                    TableTools.intCol("N2", columns[2]),
                    TableTools.intCol("N3", columns[3]));
        }
    },

    /** A market-data-like trades table mixing timestamps, symbols, prices, sizes, ids and a sparse note. */
    MIXED {
        @Override
        Table makeTable(final int numRows, final Random random) {
            final Instant[] timestamp = new Instant[numRows];
            final String[] sym = new String[numRows];
            final String[] exchange = new String[numRows];
            final double[] price = new double[numRows];
            final int[] size = new int[numRows];
            final long[] tradeId = new long[numRows];
            final String[] note = new String[numRows];

            long nanos = BASE_NANOS;
            double lastPrice = 100.0;
            for (int ii = 0; ii < numRows; ++ii) {
                nanos += 1 + random.nextInt(2_000_000);
                timestamp[ii] = Instant.ofEpochSecond(0, nanos);
                sym[ii] = SYMBOLS[random.nextInt(SYMBOLS.length)];
                exchange[ii] = EXCHANGES[random.nextInt(EXCHANGES.length)];
                lastPrice = Math.max(0.01, lastPrice + (random.nextInt(21) - 10) * 0.01);
                price[ii] = Math.round(lastPrice * 100) / 100.0;
                size[ii] = 100 * (1 + random.nextInt(50));
                tradeId[ii] = 1_000_000_000L + ii;
                note[ii] = random.nextInt(20) == 0 ? randomHex(random) : null;
            }
            return TableTools.newTable(
                    TableTools.instantCol("Timestamp", timestamp),
                    TableTools.stringCol("Sym", sym),
                    TableTools.stringCol("Exchange", exchange),
                    TableTools.doubleCol("Price", price),
                    TableTools.intCol("Size", size),
                    TableTools.longCol("TradeId", tradeId),
                    TableTools.stringCol("Note", note));
        }
    };

    private static final long BASE_NANOS = 1_767_225_600_000_000_000L; // 2026-01-01T00:00:00Z

    private static final String[] SYMBOLS = {
            "AAPL", "MSFT", "GOOG", "AMZN", "META", "NVDA", "TSLA", "BRK.B",
            "JPM", "V", "UNH", "XOM", "JNJ", "WMT", "MA", "PG",
            "HD", "CVX", "MRK", "ABBV", "KO", "PEP", "AVGO", "COST",
            "LLY", "TMO", "MCD", "CSCO", "ACN", "ABT", "DHR", "NKE"};

    private static final String[] EXCHANGES = {"NYSE", "NASDAQ", "ARCA", "BATS", "IEX", "EDGX", "MEMX", "AMEX"};

    /**
     * @param numRows the number of rows to generate
     * @return a static table of this shape, identical across calls with the same {@code numRows}
     */
    public Table makeTable(final int numRows) {
        return makeTable(numRows, new Random(0x5EED_0000L + ordinal()));
    }

    abstract Table makeTable(int numRows, Random random);

    private static String randomHex(final Random random) {
        final String hex = Long.toHexString(random.nextLong());
        return "0000000000000000".substring(hex.length()) + hex;
    }
}
