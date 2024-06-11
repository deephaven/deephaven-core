//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.context.QueryLibrary;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.regioned.TestChunkedRegionedOperations;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 1, time = 2)
@Fork(1)
public class TableWriteBenchmark {
    private static final String[] LETTERS = IntStream.range('A', 'A' + 64)
            .mapToObj(c -> String.valueOf((char) c))
            .toArray(String[]::new);

    @Param({"UNCOMPRESSED", "SNAPPY", "GZIP"})
    private String compressionCodec;

    private Table table;
    private SafeCloseable exContextCloseable;
    private Path rootPath;

    @Setup(Level.Trial)
    public void setupEnv() throws IOException {
        rootPath = Files.createTempDirectory("TableWriteBenchmark");
        final ExecutionContext context = ExecutionContext.newBuilder()
                .newQueryLibrary()
                .newQueryScope()
                .setQueryCompiler(
                        QueryCompilerImpl.create(rootPath.resolve("cache").toFile(), getClass().getClassLoader()))
                .build();
        exContextCloseable = context.open();

        final QueryScope queryScope = context.getQueryScope();
        queryScope.putParam("nowNanos", DateTimeUtils.currentClock().currentTimeNanos());
        queryScope.putParam("letters", LETTERS);

        QueryLibrary library = context.getQueryLibrary();
        library.importClass(TestChunkedRegionedOperations.SimpleExternalizable.class);
        // Serializable is already very inefficient, however SNAPPY also has an O(n^2) block write pattern and compounds
        // the terribleness. For now, we will exclude the serializable from the benchmark
        // library.importClass(TestChunkedRegionedOperations.SimpleSerializable.class);
        library.importClass(BigInteger.class);

        table = TableTools.emptyTable(1_000_000).updateView(
                "B    = ii % 1000  == 0  ? NULL_BYTE   : (byte)  ii",
                "C    = ii % 27    == 26 ? NULL_CHAR   : (char)  ('A' + ii % 27)",
                "S    = ii % 30000 == 0  ? NULL_SHORT  : (short) ii",
                "I    = ii % 512   == 0  ? NULL_INT    : (int)   ii",
                "L    = ii % 1024  == 0  ? NULL_LONG   :         ii",
                "F    = ii % 2048  == 0  ? NULL_FLOAT  : (float) (ii * 0.25)",
                "D    = ii % 4096  == 0  ? NULL_DOUBLE :         ii * 1.25",
                "Bl   = ii % 8192  == 0  ? null        :         ii % 2 == 0",
                "Sym  = ii % 64    == 0  ? null        :         Long.toString(ii % 1000)",
                "Str  = ii % 128   == 0  ? null        :         Long.toString(ii)",
                "DT   = ii % 256   == 0  ? null        :         DateTimeUtils.epochNanosToInstant(nowNanos + ii)",
                // "Ser = ii % 1024 == 0 ? null : new SimpleSerializable(ii)",
                "Ext  = ii % 1024  == 0  ? null        : new SimpleExternalizable(ii)",
                "Fix  = ii % 64    == 0  ? null        : new BigInteger(Long.toString(ii % 1000), 10)",
                "Var  = Str == null      ? null        : new BigInteger(Str, 10)");
    }

    @TearDown(Level.Trial)
    public void cleanUp() {
        FileUtils.deleteRecursively(rootPath.toFile());
        exContextCloseable.close();
    }

    @Benchmark
    public Table writeTable(@NotNull final Blackhole bh) {
        final ParquetInstructions instructions = ParquetInstructions.builder()
                .setCompressionCodecName(compressionCodec)
                .build();
        ParquetTools.writeTable(table, rootPath.resolve("table.parquet").toString(), instructions);
        return table;
    }
}
