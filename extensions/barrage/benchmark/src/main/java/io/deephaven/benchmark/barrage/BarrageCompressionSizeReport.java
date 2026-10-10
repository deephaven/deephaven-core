//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;

import java.util.List;

/**
 * Prints, as a Markdown table, the total wire size of a {@code DoGet}-style snapshot for every combination of
 * {@link DataShape}, row count and {@link MessageCodec} that {@link BarrageCompressionBenchmark} times. Every
 * configuration is verified to round trip before it is reported.
 * <p>
 * Optional arguments narrow the report: {@code shape=MIXED}, {@code numRows=65536} (each may be comma separated).
 */
public class BarrageCompressionSizeReport {
    private static final int[] DEFAULT_ROW_COUNTS = {1024, 65536, 1048576};

    public static void main(final String[] args) {
        DataShape[] shapes = DataShape.values();
        int[] rowCounts = DEFAULT_ROW_COUNTS;
        for (final String arg : args) {
            final String[] kv = arg.split("=", 2);
            if (kv.length == 2 && kv[0].equals("shape")) {
                final String[] names = kv[1].split(",");
                shapes = new DataShape[names.length];
                for (int ii = 0; ii < names.length; ++ii) {
                    shapes[ii] = DataShape.valueOf(names[ii].trim());
                }
            } else if (kv.length == 2 && kv[0].equals("numRows")) {
                final String[] counts = kv[1].split(",");
                rowCounts = new int[counts.length];
                for (int ii = 0; ii < counts.length; ++ii) {
                    rowCounts[ii] = Integer.parseInt(counts[ii].trim());
                }
            } else {
                throw new IllegalArgumentException("Unrecognized argument: " + arg);
            }
        }

        System.out.println("| Shape | Rows | Messages | Codec | Wire bytes | % of uncompressed | Ratio |");
        System.out.println("|---|---:|---:|---|---:|---:|---:|");
        try (final SafeCloseable ignored = TestExecutionContext.createForUnitTests().open()) {
            for (final DataShape shape : shapes) {
                for (final int numRows : rowCounts) {
                    report(shape, numRows);
                }
            }
        }
        System.exit(0);
    }

    private static void report(final DataShape shape, final int numRows) {
        final Table table = shape.makeTable(numRows);
        final BarrageCompressionHarness harness = new BarrageCompressionHarness(table);
        final List<byte[]> flightData = harness.serialize();
        final long baseline = totalBytes(flightData);
        for (final MessageCodec codec : MessageCodec.values()) {
            final List<byte[]> wire = BarrageCompressionHarness.compressAll(flightData, codec);
            harness.verify(flightData, wire, codec);
            final long bytes = totalBytes(wire);
            System.out.printf("| %s | %,d | %,d | %s | %,d | %.1f%% | %.2fx |%n",
                    shape, numRows, wire.size(), codec, bytes, 100.0 * bytes / baseline, (double) baseline / bytes);
        }
    }

    private static long totalBytes(final List<byte[]> messages) {
        long total = 0;
        for (final byte[] message : messages) {
            total += message.length;
        }
        return total;
    }
}
