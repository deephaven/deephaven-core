//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Times gRPC message compression ({@link MessageCodec}) of a full {@code DoGet}-style snapshot of a static table. Each
 * invocation processes the whole table; run {@link BarrageCompressionSizeReport} for the matching wire sizes.
 * <ul>
 * <li>{@link #compress} and {@link #decompress} time only the compression work, starting from and ending at serialized
 * bytes.</li>
 * <li>{@link #serverWrite} and {@link #clientRead} time the full path, including Barrage serialization of the table and
 * deserialization back into column chunks, so compression can be judged as a share of the total.</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class BarrageCompressionBenchmark {

    @Param({"RANDOM_DOUBLE", "SEQUENTIAL_LONG", "LOW_CARDINALITY_STRING", "HIGH_CARDINALITY_STRING",
            "NULL_HEAVY_INT", "MIXED"})
    private DataShape shape;

    @Param({"1024", "65536", "1048576"})
    private int numRows;

    @Param({"GZIP", "ZSTD", "SNAPPY"})
    private MessageCodec codec;

    private SafeCloseable executionContext;
    private BarrageCompressionHarness harness;
    /** the uncompressed messages {@code DoGet} sends */
    private List<byte[]> flightData;
    /** {@link #flightData} after compression */
    private List<byte[]> wire;

    @Setup(Level.Trial)
    public void setup() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        final Table table = shape.makeTable(numRows);
        harness = new BarrageCompressionHarness(table);
        flightData = harness.serialize();
        wire = BarrageCompressionHarness.compressAll(flightData, codec);
        harness.verify(flightData, wire, codec);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        executionContext.close();
    }

    /** Server-side compression only: serialized {@code FlightData} in, wire bytes out. */
    @Benchmark
    public void compress(@NotNull final Blackhole bh) {
        for (final byte[] message : flightData) {
            bh.consume(codec.compress(message));
        }
    }

    /** Client-side decompression only: wire bytes in, serialized {@code FlightData} out. */
    @Benchmark
    public void decompress(@NotNull final Blackhole bh) {
        for (final byte[] message : wire) {
            bh.consume(codec.decompress(message));
        }
    }

    /** The whole server path: snapshot and serialize the table, then compress. */
    @Benchmark
    public void serverWrite(@NotNull final Blackhole bh) {
        for (final byte[] message : harness.serialize()) {
            bh.consume(codec.compress(message));
        }
    }

    /** The whole client path: decompress, then deserialize into column chunks. */
    @Benchmark
    public void clientRead(@NotNull final Blackhole bh) {
        harness.deserialize(wire, codec, message -> {
            try (message) {
                bh.consume(message.rowsIncluded.size());
            }
        });
    }
}
