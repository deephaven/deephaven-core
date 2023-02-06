/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmarking;

import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.DataDir;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.benchmarking.generator.*;
import io.deephaven.benchmarking.impl.PersistentBenchmarkTableBuilder;
import io.deephaven.benchmarking.impl.InMemoryBenchmarkTableBuilder;
import io.deephaven.benchmarking.impl.TableBackedBenchmarkTableBuilder;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * An entry point to get instances of {@link BenchmarkTableBuilder}s.
 */
@ScriptApi
public class BenchmarkTools {

    private static final List<ColumnDefinition<?>> COMMON_RESULT_COLUMNS = Arrays.asList(
            ColumnDefinition.ofString("Benchmark"),
            ColumnDefinition.ofString("Mode"),
            ColumnDefinition.ofInt("Iteration"),
            ColumnDefinition.ofString("Params"));


    /**
     * Get a new {@link PersistentBenchmarkTableBuilder}
     *
     * @param name The name of the table.
     * @param size The desired size of the table.
     * @return a new {@link PersistentBenchmarkTableBuilder}.
     */

    @ScriptApi
    public static PersistentBenchmarkTableBuilder persistentTableBuilder(String name, int size) {
        return new PersistentBenchmarkTableBuilder(name, size);
    }

    /**
     * Get a new {@link InMemoryBenchmarkTableBuilder}
     *
     * @param name The name of the table.
     * @param size The desired size of the table.
     * @return a new {@link BenchmarkTableBuilder}.
     */

    @ScriptApi
    public static BenchmarkTableBuilder inMemoryTableBuilder(String name, int size) {
        return new InMemoryBenchmarkTableBuilder(name, size);
    }

    /**
     * Create a new {@link BenchmarkTableBuilder} that will apply generated columns as an
     * {@link Table#update(String...)} call to the source table.
     *
     * @param name The name of the table.
     * @param fromTable The table to use as a base.
     * @return a new {@link BenchmarkTableBuilder}.
     */
    @ScriptApi
    BenchmarkTableBuilder tableBuilder(String name, Table fromTable) {
        return new TableBackedBenchmarkTableBuilder(name, fromTable);
    }

    /**
     * Create an enumerated {@link ColumnGenerator<String>}, selecting values from the enum randomly.
     *
     * @param name The name of the column
     * @param nVals The number of values in the enumeration
     * @param minLen The minimum length of a value
     * @param maxLen The maximum length of a value
     * @param seed The RNG seed to use to create the enumerated values
     * @return a {@link ColumnGenerator<String>} suitable for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<String> stringCol(String name, int nVals, int minLen, int maxLen, long seed) {
        return stringCol(name, nVals, minLen, maxLen, seed, EnumStringColumnGenerator.Mode.Random);
    }

    /**
     * <p>
     * Create an enumerated {@link ColumnGenerator<String>} selecting values from the enum using the selected mode.
     * </p>
     *
     * <ul>
     * <li><i>{@link EnumStringColumnGenerator.Mode#Random}</i> - Select enum values randomly</li>
     * <li><i>{@link EnumStringColumnGenerator.Mode#Rotate}</i> - Select enum values in order, and wrap around</li>
     * </ul>
     *
     * @param name The name of the column
     * @param nVals The number of values in the enumeration
     * @param minLen The minimum length of a value
     * @param maxLen The maximum length of a value
     * @param seed The RNG seed to use to create the enumerated values
     * @param mode The selection mode to use.
     * @return a {@link ColumnGenerator<String>} suitable for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<String> stringCol(String name, int nVals, int minLen, int maxLen, long seed,
            EnumStringColumnGenerator.Mode mode) {
        return new EnumStringColumnGenerator(name, nVals, minLen, maxLen, seed, mode);
    }

    /**
     * Create a {@link ColumnGenerator<String>} that generates random strings constrained by length.
     *
     * @param name The name of the column
     * @param minLen the minimum length of a value
     * @param maxLen the maximum length of a value
     * @return a {@link ColumnGenerator<String>} suitable for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<String> stringCol(String name, int minLen, int maxLen) {
        return new RandomStringColumnGenerator(name, minLen, maxLen);
    }

    /**
     * Create a {@link ColumnGenerator<Character>} that generates values between min and max
     *
     * @param name The name of the column
     * @param min the minimum value
     * @param max the maximum value
     * @return a {@link ColumnGenerator<Character>} for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<Character> charCol(String name, char min, char max) {
        return new CharColumnGenerator(name, min, max);
    }

    /**
     * @param name The name of the column
     * @return a {@link ColumnGenerator< DateTime >} for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<DateTime> dateCol(String name) {
        return new DateColumnGenerator(name);
    }

    /**
     * @param name The name of the column
     * @param min the minimum value
     * @param max the maximum value
     * @return a {@link ColumnGenerator< DateTime >} for use with
     *         {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static ColumnGenerator<DateTime> dateCol(String name, DateTime min, DateTime max) {
        return new DateColumnGenerator(name, min, max);
    }

    /**
     * Create a {@link ColumnGenerator<T>} that generates a random number of the desired type.
     *
     * @param name The name of the column
     * @param type The type of number
     * @param <T> The type of number
     * @return a {@link ColumnGenerator<T>} for use with {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static <T extends Number> ColumnGenerator<T> numberCol(String name, Class<T> type) {
        return new RandomNumColumnGenerator<>(type, name);
    }

    /**
     * Create a {@link ColumnGenerator<T>} that generates a random number of the desired type within a range.
     *
     * @param name The name of the column
     * @param type The type of number
     * @param min The minimum value
     * @param max The maximum value
     * @param <T> The type of number
     * @return a {@link ColumnGenerator<T>} for use with {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static <T extends Number> ColumnGenerator<T> numberCol(String name, Class<T> type, double min, double max) {
        return new RandomNumColumnGenerator<>(type, name, min, max);
    }

    /**
     * Create a {@link ColumnGenerator<T>} that generates a monotonically increasing number of the desired type.
     *
     * @param name The name of the column
     * @param type The type of number
     * @param start The starting value
     * @param step The value to step by
     * @param <T> The type of number
     * @return a {@link ColumnGenerator<T>} for use with {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static <T extends Number> ColumnGenerator<T> seqNumberCol(String name, Class<T> type, double start,
            double step) {
        return new SequentialNumColumnGenerator<>(type, name, start, step);
    }

    /**
     * <p>
     * Create a {@link ColumnGenerator<T>} that generates a number of the desired type which steps based on the input
     * {@link SequentialNumColumnGenerator.Mode}.
     * </p>
     *
     * <ul>
     * <li><i>{@link SequentialNumColumnGenerator.Mode#NoLimit}</i> - Monotonically increasing with no limit</li>
     * <li><i>{@link SequentialNumColumnGenerator.Mode#RollAtLimit}</i> - Roll over to the start value when the limit is
     * reached</li>
     * <li><i>{@link SequentialNumColumnGenerator.Mode#ReverseAtLimit}</i> - Change increment direction when the limit
     * is reached</li>
     * </ul>
     *
     * @param name The name of the column
     * @param type The type of number
     * @param start The starting value
     * @param step The value to step by
     * @param max the limit value
     * @param mode What to do when the max is reached.
     * @param <T> The type of number
     * @return a {@link ColumnGenerator<T>} for use with {@link BenchmarkTableBuilder#addColumn(ColumnGenerator)}
     */
    @ScriptApi
    public static <T extends Number> ColumnGenerator<T> seqNumberCol(String name, Class<T> type, double start,
            double step, double max, SequentialNumColumnGenerator.Mode mode) {
        return new SequentialNumColumnGenerator<>(type, name, start, step, max, mode);
    }

    /**
     * Strip a benchmark name of the format x.y.z.a.b.c.d.BenchmarkClass.method to BenchmarkClass.method
     *
     * @param benchmark The full benchmark name
     * @return The stripped version
     */
    public static String stripName(String benchmark) {
        final String[] byParts = benchmark.split("\\.");
        if (byParts.length < 3) {
            return benchmark;
        }

        return byParts[byParts.length - 2] + "." + byParts[byParts.length - 1];
    }

    public static String buildParameterString(BenchmarkParams params) {
        return params.getParamsKeys().stream().map(params::getParam).collect(Collectors.joining(";"));
    }

    public static Path dataDir() {
        return DataDir.get();
    }

    public static final String DETAIL_LOG_PREFIX = "Details.";

    public static String getDetailOutputPath(String benchmarkName) {
        return DETAIL_LOG_PREFIX + benchmarkName;
    }

    public static TableDefinition getLogDefinitionWithExtra(List<ColumnDefinition<?>> columnsToAdd) {
        final List<ColumnDefinition<?>> columns = new ArrayList<>(COMMON_RESULT_COLUMNS);
        columns.addAll(columnsToAdd);

        return TableDefinition.of(columns);
    }

    public static String getStrippedBenchmarkName(BenchmarkParams params) {
        return stripName(params.getBenchmark());
    }

    public static String getModeString(BenchmarkParams params) {
        return params.getMode().toString();
    }

    private static final double SPARSITY_FUDGE_FACTOR = 1.2;

    public static int sizeWithSparsity(int size, int sparsity) {
        if (sparsity <= 0 || sparsity > 100) {
            throw new IllegalStateException("Sparsity must be in the range of 1 through 100");
        }

        return sparsity == 100 ? size : (int) Math.ceil(size * SPARSITY_FUDGE_FACTOR / (sparsity / 100.0));
    }

    public static Table applySparsity(Table table, int size, int sparsity, long seed) {
        if (sparsity == 100) {
            return table;
        }
        if (sparsity <= 0 || sparsity > 100) {
            throw new IllegalStateException("Sparsity must be in the range of 1 through 100");
        }
        Random random = new Random(seed);
        ExecutionContext.getContext().getQueryScope().putParam("__random__", random);
        return table.where("__random__.nextInt(100) < " + sparsity).head(size);
    }

}
