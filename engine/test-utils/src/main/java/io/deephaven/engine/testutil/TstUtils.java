//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.LivenessStateException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.NoSuchColumnException.Type;
import io.deephaven.engine.table.impl.PrevColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.util.LongColumnSourceRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.testutil.rowset.RowSetTstUtils;
import io.deephaven.engine.testutil.sources.ByteTestSource;
import io.deephaven.engine.testutil.sources.DoubleTestSource;
import io.deephaven.engine.testutil.sources.FloatTestSource;
import io.deephaven.engine.testutil.sources.ImmutableByteTestSource;
import io.deephaven.engine.testutil.sources.ImmutableCharTestSource;
import io.deephaven.engine.testutil.sources.ImmutableColumnHolder;
import io.deephaven.engine.testutil.sources.CharTestSource;
import io.deephaven.engine.testutil.sources.ImmutableDoubleTestSource;
import io.deephaven.engine.testutil.sources.ImmutableFloatTestSource;
import io.deephaven.engine.testutil.sources.ImmutableInstantTestSource;
import io.deephaven.engine.testutil.sources.ImmutableIntTestSource;
import io.deephaven.engine.testutil.sources.ImmutableLongTestSource;
import io.deephaven.engine.testutil.sources.ImmutableObjectTestSource;
import io.deephaven.engine.testutil.sources.ImmutableShortTestSource;
import io.deephaven.engine.testutil.sources.InstantTestSource;
import io.deephaven.engine.testutil.sources.IntTestSource;
import io.deephaven.engine.testutil.sources.LongTestSource;
import io.deephaven.engine.testutil.sources.ObjectTestSource;
import io.deephaven.engine.testutil.sources.ShortTestSource;
import io.deephaven.engine.testutil.sources.TestColumnSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.stringset.HashStringSet;
import io.deephaven.stringset.StringSet;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.type.TypeUtils;
import junit.framework.AssertionFailedError;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility functions to create and update test tables, compare results, and otherwise make unit testing more pleasant.
 */
@SuppressWarnings("unused")
public class TstUtils {
    public static boolean SHORT_TESTS = Configuration.getInstance()
            .getBooleanForClassWithDefault(TstUtils.class, "shortTests", false);

    public static int scaleToDesiredTestLength(final int maxIter) {
        if (!SHORT_TESTS) {
            return maxIter;
        }
        final double shortTestFactor = 0.2;
        return (int) Math.ceil(maxIter * shortTestFactor);
    }

    /**
     * Create a column holder from the given chunk.
     *
     * @param name the name of the column
     * @param type the type of the column
     * @param componentType the component type of the column if applicable
     * @param chunkData the data in an chunk for this column
     * @return the new ColumnHolder
     */
    public static <T> ColumnHolder<T> columnHolderForChunk(
            String name, Class<T> type, Class<?> componentType, Chunk<Values> chunkData) {
        return ColumnHolder.makeForChunk(name, type, componentType, false, chunkData);
    }

    /**
     * Create an indexed column holder from the given chunk.
     *
     * @param name the name of the column
     * @param type the type of the column
     * @param componentType the component type of the column if applicable
     * @param chunkData the data in an chunk for this column
     * @return the new ColumnHolder with the indexed attribute set
     */
    public static <T> ColumnHolder<T> indexedColumnHolderForChunk(
            String name, Class<T> type, Class<?> componentType, Chunk<Values> chunkData) {
        return ColumnHolder.makeForChunk(name, type, componentType, true, chunkData);
    }

    /**
     * A shorthand for {@link RowSetFactory#fromKeys} for use in unit tests.
     *
     * @param keys the keys of the new RowSet
     * @return a new RowSet with the given keys
     */
    public static WritableRowSet i(long... keys) {
        return RowSetFactory.fromKeys(keys);
    }

    public static void addToTable(final Table table, final RowSet rowSet, final ColumnHolder<?>... columnHolders) {
        if (rowSet.isEmpty()) {
            return;
        }

        Require.requirement(table.isRefreshing(), "table.isRefreshing()");
        final Set<String> usedNames = new HashSet<>();
        for (ColumnHolder<?> columnHolder : columnHolders) {
            if (columnHolder == null) {
                continue;
            }

            if (!usedNames.add(columnHolder.name)) {
                throw new IllegalStateException("Added to the same column twice!");
            }
            final ColumnSource<?> columnSource = table.getColumnSource(columnHolder.name);
            if (columnHolder.size() == 0) {
                continue;
            }

            if (rowSet.size() != columnHolder.size()) {
                throw new IllegalArgumentException(columnHolder.name + ": Invalid data addition: rowSet="
                        + rowSet.size() + ", arraySize=" + columnHolder.size());
            }

            if (!(columnSource instanceof InstantTestSource && columnHolder.dataType == long.class)
                    && !(columnSource.getType() == Boolean.class && columnHolder.dataType == Boolean.class)
                    && (columnSource.getType() != TypeUtils.getUnboxedTypeIfBoxed(columnHolder.dataType))) {
                throw new UnsupportedOperationException(columnHolder.name + ": Adding invalid type: source.getType()="
                        + columnSource.getType() + ", columnHolder=" + columnHolder.dataType);
            }

            if (columnSource instanceof TestColumnSource) {
                final TestColumnSource<?> testSource = (TestColumnSource<?>) columnSource;
                testSource.add(rowSet, columnHolder.getChunk());
            } else {
                throw new IllegalStateException(
                        "Can not add to tables with unknown column sources: " + columnSource.getClass());
            }
        }

        NoSuchColumnException.throwIf(
                usedNames,
                table.getDefinition().getColumnNameSet(),
                "Not all columns were populated, missing [%s], available [%s]",
                Type.MISSING,
                Type.AVAILABLE);

        table.getRowSet().writableCast().insert(rowSet);
        if (table.isFlat()) {
            Assert.assertion(table.getRowSet().isFlat(), "table.build().isFlat()", table.getRowSet(),
                    "table.build()", rowSet, "rowSet");
        }
    }

    public static void removeRows(Table table, RowSet rowSet) {
        Require.requirement(table.isRefreshing(), "table.isRefreshing()");
        table.getRowSet().writableCast().remove(rowSet);
        if (table.isFlat()) {
            Assert.assertion(table.getRowSet().isFlat(), "table.build().isFlat()", table.getRowSet(),
                    "table.build()", rowSet, "rowSet");
        }
        for (ColumnSource<?> columnSource : table.getColumnSources()) {
            if (columnSource instanceof TestColumnSource) {
                final TestColumnSource<?> testColumnSource = (TestColumnSource<?>) columnSource;
                if (!columnSource.isImmutable()) {
                    testColumnSource.remove(rowSet);
                }
            } else {
                throw new IllegalStateException("Not a test column source: " + columnSource);
            }
        }
    }

    @SafeVarargs
    public static <T> ColumnHolder<T> colIndexed(String name, T... data) {
        return ColumnHolder.createColumnHolder(name, true, data);
    }

    public static ColumnHolder<String> getRandomStringCol(String colName, int size, Random random) {
        final String[] data = new String[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<String[]> getRandomStringArrayCol(String colName, int size, Random random, int maxSz) {
        final String[][] data = new String[size][];
        for (int i = 0; i < data.length; i++) {
            final String[] v = new String[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<StringSet> getRandomStringSetCol(String colName, int size, Random random, int maxSz) {
        final StringSet[] data = new StringSet[size];
        for (int i = 0; i < data.length; i++) {
            final String[] v = new String[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = Long.toString(random.nextLong(), 'z' - 'a' + 10);
            }
            data[i] = new HashStringSet(Arrays.asList(v));
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<Integer> getRandomIntCol(String colName, int size, Random random) {
        final Integer[] data = new Integer[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(1000);
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<Double> getRandomDoubleCol(String colName, int size, Random random) {
        final Double[] data = new Double[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextDouble();
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<Float> getRandomFloatCol(String colName, int size, Random random) {
        final float[] data = new float[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextFloat();
        }
        return new ColumnHolder<>(colName, false, data);
    }

    public static ColumnHolder<Short> getRandomShortCol(String colName, int size, Random random) {
        final short[] data = new short[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (short) random.nextInt(Short.MAX_VALUE);
        }
        return new ColumnHolder<>(colName, false, data);
    }

    public static ColumnHolder<Long> getRandomLongCol(String colName, int size, Random random) {
        final long[] data = new long[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextLong();
        }
        return new ColumnHolder<>(colName, false, data);
    }

    public static ColumnHolder<Boolean> getRandomBooleanCol(String colName, int size, Random random) {
        final Boolean[] data = new Boolean[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextBoolean();
        }
        return ColumnHolder.createColumnHolder(colName, false, data);
    }

    public static ColumnHolder<Character> getRandomCharCol(String colName, int size, Random random) {
        final char[] data = new char[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) random.nextInt();
        }
        return new ColumnHolder<>(colName, false, data);
    }

    public static ColumnHolder<Byte> getRandomByteCol(String colName, int size, Random random) {
        final byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) random.nextInt();
        }
        return new ColumnHolder<>(colName, false, data);
    }

    public static ColumnHolder<byte[]> getRandomByteArrayCol(String colName, int size, Random random, int maxSz) {
        final byte[][] data = new byte[size][];
        for (int i = 0; i < size; i++) {
            final byte[] b = new byte[random.nextInt(maxSz)];
            random.nextBytes(b);
            data[i] = b;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<Boolean[]> getRandomBooleanArrayCol(String colName, int size, Random random, int maxSz) {
        final Boolean[][] data = new Boolean[size][];
        for (int i = 0; i < size; i++) {
            final Boolean[] v = new Boolean[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextBoolean();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<int[]> getRandomIntArrayCol(String colName, int size, Random random, int maxSz) {
        final int[][] data = new int[size][];
        for (int i = 0; i < size; i++) {
            final int[] v = new int[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextInt();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<long[]> getRandomLongArrayCol(String colName, int size, Random random, int maxSz) {
        final long[][] data = new long[size][];
        for (int i = 0; i < size; i++) {
            final long[] v = new long[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextLong();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<short[]> getRandomShortArrayCol(String colName, int size, Random random, int maxSz) {
        final short[][] data = new short[size][];
        for (int i = 0; i < size; i++) {
            final short[] v = new short[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = (short) random.nextInt();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<double[]> getRandomDoubleArrayCol(String colName, int size, Random random, int maxSz) {
        final double[][] data = new double[size][];
        for (int i = 0; i < size; i++) {
            final double[] v = new double[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextDouble();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<float[]> getRandomFloatArrayCol(String colName, int size, Random random, int maxSz) {
        final float[][] data = new float[size][];
        for (int i = 0; i < size; i++) {
            final float[] v = new float[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = random.nextFloat();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<char[]> getRandomCharArrayCol(String colName, int size, Random random, int maxSz) {
        final char[][] data = new char[size][];
        for (int i = 0; i < size; i++) {
            final char[] v = new char[random.nextInt(maxSz)];
            for (int j = 0; j < v.length; j++) {
                v[j] = (char) random.nextInt();
            }
            data[i] = v;
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<BigDecimal> getRandomBigDecimalCol(String colName, int size, Random random) {
        final BigDecimal[] data = new BigDecimal[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = BigDecimal.valueOf(random.nextDouble());
        }
        return TableTools.col(colName, data);
    }

    public static ColumnHolder<Instant> getRandomInstantCol(String colName, int size, Random random) {
        final Instant[] data = new Instant[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = DateTimeUtils.epochAutoToInstant(random.nextLong());
        }
        return ColumnHolder.createColumnHolder(colName, false, data);
    }

    public static void validate(final EvalNuggetInterface[] en) {
        validate("", en);
    }

    public static void validate(final String ctxt, final EvalNuggetInterface[] en) {
        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println();
            System.out.println("================ NEXT ITERATION ================");
        }
        for (int i = 0; i < en.length; i++) {
            try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
                if (RefreshingTableTestCase.printTableUpdates) {
                    if (i != 0) {
                        System.out.println("================ NUGGET (" + i + ") ================");
                    }
                    try {
                        en[i].show();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    System.out.println();
                }
                en[i].validate(ctxt + " en_i = " + i);
            }
            en[i].releaseRecomputed();
        }
    }

    static WritableRowSet getInitialIndex(int size, Random random) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        long firstKey = 10;
        for (int i = 0; i < size; i++) {
            builder.appendKey(firstKey = firstKey + 1 + random.nextInt(3));
        }
        return builder.build();
    }

    public static WritableRowSet selectSubIndexSet(int size, RowSet sourceRowSet, Random random) {
        Assert.assertion(size <= sourceRowSet.size(), "size <= sourceRowSet.size()", size, "size", sourceRowSet,
                "sourceRowSet.size()");

        // generate an array that is the size of our RowSet, then shuffle it, and those are the positions we'll pick
        final Integer[] positions = new Integer[(int) sourceRowSet.size()];
        for (int ii = 0; ii < positions.length; ++ii) {
            positions[ii] = ii;
        }
        Collections.shuffle(Arrays.asList(positions), random);

        // now create a RowSet with each of our selected positions
        final RowSetBuilderRandom resultBuilder = RowSetFactory.builderRandom();
        for (int ii = 0; ii < size; ++ii) {
            resultBuilder.addKey(sourceRowSet.get(positions[ii]));
        }

        return resultBuilder.build();
    }

    public static RowSet newIndex(int targetSize, RowSet sourceRowSet, Random random) {
        final long maxKey = (sourceRowSet.size() == 0 ? 0 : sourceRowSet.lastRowKey());
        final long emptySlots = maxKey - sourceRowSet.size();
        final int slotsToFill = Math.min(
                Math.min((int) (Math.max(0.0, ((random.nextGaussian() / 0.1) + 0.9)) * emptySlots), targetSize),
                (int) emptySlots);

        final WritableRowSet fillIn =
                selectSubIndexSet(slotsToFill,
                        RowSetFactory.fromRange(0, maxKey).minus(sourceRowSet), random);

        final int endSlots = targetSize - (int) fillIn.size();

        double density = ((random.nextGaussian() / 0.25) + 0.5);
        density = Math.max(density, 0.1);
        density = Math.min(density, 1);
        final long rangeSize = (long) ((1.0 / density) * endSlots);
        final RowSet expansion =
                selectSubIndexSet(endSlots,
                        RowSetFactory.fromRange(maxKey + 1, maxKey + rangeSize + 1), random);

        fillIn.insert(expansion);

        Assert.assertion(fillIn.size() == targetSize, "fillIn.size() == targetSize", fillIn.size(), "fillIn.size()",
                targetSize, "targetSize", endSlots, "endSlots", slotsToFill, "slotsToFill");

        return fillIn;
    }

    public static ColumnInfo<?, ?>[] initColumnInfos(String[] names, TestDataGenerator<?, ?>... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException(
                    "names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo<?, ?>[] result = new ColumnInfo[names.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new ColumnInfo<>(generators[i], names[i]);
        }
        return result;
    }

    public static ColumnInfo<?, ?>[] initColumnInfos(String[] names, ColumnInfo.ColAttributes[] attributes,
            TestDataGenerator<?, ?>... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException(
                    "names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo<?, ?>[] result = new ColumnInfo[names.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new ColumnInfo<>(generators[i], names[i], attributes);
        }
        return result;
    }

    public static ColumnInfo<?, ?>[] initColumnInfos(String[] names, List<List<ColumnInfo.ColAttributes>> attributes,
            TestDataGenerator<?, ?>... generators) {
        if (names.length != generators.length) {
            throw new IllegalArgumentException(
                    "names and generator lengths mismatch: " + names.length + " != " + generators.length);
        }

        final ColumnInfo<?, ?>[] result = new ColumnInfo[names.length];
        for (int ii = 0; ii < result.length; ii++) {
            result[ii] = new ColumnInfo<>(generators[ii], names[ii],
                    attributes.get(ii).toArray(ColumnInfo.ZERO_LENGTH_COLUMN_ATTRIBUTES_ARRAY));
        }
        return result;
    }

    public static QueryTable getTable(int size, Random random, ColumnInfo<?, ?>[] columnInfos) {
        return getTable(true, size, random, columnInfos);
    }

    public static QueryTable getTable(
            final boolean refreshing,
            final int size,
            @NotNull final Random random,
            @NotNull final ColumnInfo<?, ?>[] columnInfos) {
        final TrackingWritableRowSet rowSet = getInitialIndex(size, random).toTracking();
        final ColumnHolder<?>[] columnHolders = new ColumnHolder[columnInfos.length];
        for (int i = 0; i < columnInfos.length; i++) {
            columnHolders[i] = columnInfos[i].generateInitialColumn(rowSet, random);
        }
        if (refreshing) {
            return testRefreshingTable(rowSet, columnHolders);
        } else {
            return testTable(rowSet, columnHolders);
        }
    }

    private static QueryTable testTable(
            final boolean refreshing,
            final boolean flat,
            @NotNull final TrackingRowSet rowSet,
            @NotNull final ColumnHolder<?>... columnHolders) {
        final Map<String, ColumnSource<?>> columns = getColumnSourcesFromHolders(rowSet, columnHolders);
        final QueryTable queryTable = new QueryTable(rowSet, columns);
        queryTable.setAttribute(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true);
        if (refreshing) {
            queryTable.setRefreshing(true);
        }
        if (flat) {
            Assert.assertion(rowSet.isFlat(), "rowSet.isFlat()");
            queryTable.setFlat();
        }

        // Add indexes for the indexed columns.
        for (ColumnHolder<?> columnHolder : columnHolders) {
            if (columnHolder.indexed) {
                // This mechanism is only safe in a reachability/liveness sense if we're enclosed in a LivenessScope
                // that enforces strong reachability.
                DataIndexer.getOrCreateDataIndex(queryTable, columnHolder.name);
            }
        }

        return queryTable;
    }

    public static QueryTable testTable(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final ColumnHolder<?>... columnHolders) {
        return testTable(false, false, rowSet, columnHolders);
    }

    public static QueryTable testRefreshingTable(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final ColumnHolder<?>... columnHolders) {
        return testTable(true, false, rowSet, columnHolders);
    }

    public static QueryTable testFlatRefreshingTable(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final ColumnHolder<?>... columnHolders) {
        return testTable(true, true, rowSet, columnHolders);
    }

    private static QueryTable testTable(final boolean refreshing, @NotNull final ColumnHolder<?>... columnHolders) {
        final WritableRowSet rowSet = RowSetFactory.flat(columnHolders[0].size());
        return testTable(refreshing, false, rowSet.toTracking(), columnHolders);
    }

    public static QueryTable testTable(@NotNull final ColumnHolder<?>... columnHolders) {
        return testTable(false, columnHolders);
    }

    public static QueryTable testRefreshingTable(@NotNull final ColumnHolder<?>... columnHolders) {
        return testTable(true, columnHolders);
    }

    @NotNull
    private static Map<String, ColumnSource<?>> getColumnSourcesFromHolders(
            TrackingRowSet rowSet, ColumnHolder<?>[] columnHolders) {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        for (ColumnHolder<?> columnHolder : columnHolders) {
            columns.put(columnHolder.name, getTestColumnSource(rowSet, columnHolder));
        }
        return columns;
    }

    public static ColumnSource<?> getTestColumnSource(RowSet rowSet, ColumnHolder<?> columnHolder) {
        return getTestColumnSourceFromChunk(rowSet, columnHolder, columnHolder.getChunk());
    }

    private static <T> ColumnSource<T> getTestColumnSourceFromChunk(
            RowSet rowSet, ColumnHolder<T> columnHolder, Chunk<Values> chunkData) {
        final AbstractColumnSource<T> result;
        if (columnHolder instanceof ImmutableColumnHolder) {
            final Class<?> unboxedType = TypeUtils.getUnboxedTypeIfBoxed(columnHolder.dataType);
            if (unboxedType == char.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableCharTestSource(rowSet, chunkData);
            } else if (unboxedType == byte.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableByteTestSource(rowSet, chunkData);
            } else if (unboxedType == short.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableShortTestSource(rowSet, chunkData);
            } else if (unboxedType == int.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableIntTestSource(rowSet, chunkData);
            } else if (unboxedType == long.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableLongTestSource(rowSet, chunkData);
            } else if (unboxedType == float.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableFloatTestSource(rowSet, chunkData);
            } else if (unboxedType == double.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableDoubleTestSource(rowSet, chunkData);
            } else if (unboxedType == Instant.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ImmutableInstantTestSource(rowSet, chunkData);
            } else {
                result = new ImmutableObjectTestSource<>(columnHolder.dataType, rowSet, chunkData);
            }
        } else {
            final Class<?> unboxedType = TypeUtils.getUnboxedTypeIfBoxed(columnHolder.dataType);
            if (unboxedType == char.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new CharTestSource(rowSet, chunkData);
            } else if (unboxedType == byte.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ByteTestSource(rowSet, chunkData);
            } else if (unboxedType == short.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new ShortTestSource(rowSet, chunkData);
            } else if (unboxedType == int.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new IntTestSource(rowSet, chunkData);
            } else if (unboxedType == long.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new LongTestSource(rowSet, chunkData);
            } else if (unboxedType == float.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new FloatTestSource(rowSet, chunkData);
            } else if (unboxedType == double.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new DoubleTestSource(rowSet, chunkData);
            } else if (unboxedType == Instant.class) {
                // noinspection unchecked
                result = (AbstractColumnSource<T>) new InstantTestSource(rowSet, chunkData);
            } else {
                result = new ObjectTestSource<>(columnHolder.dataType, rowSet, chunkData);
            }
        }
        return result;
    }

    public static Table prevTableColumnSources(Table table) {
        final TrackingWritableRowSet rowSet = table.getRowSet().copyPrev().toTracking();
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        table.getColumnSourceMap().forEach((k, cs) -> {
            columnSourceMap.put(k, new PrevColumnSource<>(cs));
        });
        return new QueryTable(rowSet, columnSourceMap);
    }

    public static Table prevTable(Table table) {
        final RowSet rowSet = table.getRowSet().copyPrev();

        final List<ColumnHolder<?>> cols = new ArrayList<>();
        for (Map.Entry<String, ? extends ColumnSource<?>> mapEntry : table.getColumnSourceMap().entrySet()) {
            final String name = mapEntry.getKey();
            final ColumnSource<?> columnSource = mapEntry.getValue();
            final List<Object> data = new ArrayList<>();

            for (final RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
                final long key = it.nextLong();
                final Object item = columnSource.getPrev(key);
                data.add(item);
            }

            if (columnSource.getType() == int.class) {
                cols.add(new ColumnHolder<>(name, false, data.stream()
                        .mapToInt(x -> x == null ? io.deephaven.util.QueryConstants.NULL_INT : (int) x).toArray()));
            } else if (columnSource.getType() == long.class) {
                cols.add(new ColumnHolder<>(name, false, data.stream()
                        .mapToLong(x -> x == null ? io.deephaven.util.QueryConstants.NULL_LONG : (long) x).toArray()));
            } else if (columnSource.getType() == boolean.class) {
                cols.add(ColumnHolder.createColumnHolder(name, false,
                        data.stream().map(x -> (Boolean) x).toArray(Boolean[]::new)));
            } else if (columnSource.getType() == String.class) {
                cols.add(ColumnHolder.createColumnHolder(name, false,
                        data.stream().map(x -> (String) x).toArray(String[]::new)));
            } else if (columnSource.getType() == double.class) {
                cols.add(new ColumnHolder<>(name, false,
                        data.stream()
                                .mapToDouble(x -> x == null ? io.deephaven.util.QueryConstants.NULL_DOUBLE : (double) x)
                                .toArray()));
            } else if (columnSource.getType() == float.class) {
                final float[] floatArray = new float[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    floatArray[ii] = value == null ? QueryConstants.NULL_FLOAT : (float) value;
                }
                cols.add(new ColumnHolder<>(name, false, floatArray));
            } else if (columnSource.getType() == char.class) {
                final char[] charArray = new char[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    charArray[ii] = value == null ? QueryConstants.NULL_CHAR : (char) value;
                }
                cols.add(new ColumnHolder<>(name, false, charArray));
            } else if (columnSource.getType() == byte.class) {
                final byte[] byteArray = new byte[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    byteArray[ii] = value == null ? QueryConstants.NULL_BYTE : (byte) value;
                }
                cols.add(new ColumnHolder<>(name, false, byteArray));
            } else if (columnSource.getType() == short.class) {
                final short[] shortArray = new short[data.size()];
                for (int ii = 0; ii < data.size(); ++ii) {
                    final Object value = data.get(ii);
                    shortArray[ii] = value == null ? QueryConstants.NULL_SHORT : (short) value;
                }
                cols.add(new ColumnHolder<>(name, false, shortArray));
            } else {
                // noinspection unchecked
                cols.add(new ColumnHolder(name, columnSource.getType(), columnSource.getComponentType(), false,
                        data.toArray((Object[]) Array.newInstance(columnSource.getType(), data.size()))));
            }
        }

        return TableTools.newTable(cols.toArray(ColumnHolder.ZERO_LENGTH_COLUMN_HOLDER_ARRAY));
    }


    public static long getRandom(Random random, int bits) {
        final long value = random.nextLong();
        return bits >= 64 ? value : ((1L << bits) - 1L) & value;
    }

    public static void assertRowSetEquals(@NotNull final RowSet expected, @NotNull final RowSet actual) {
        try {
            TestCase.assertEquals(expected, actual);
        } catch (AssertionFailedError error) {
            System.err.println("TrackingWritableRowSet equality check failed:"
                    + "\n\texpected: " + expected
                    + "\n\tactual: " + actual
                    + "\n\terror: " + error);
            throw error;
        }
    }

    public static void assertTableEquals(@NotNull final Table expected, @NotNull final Table actual,
            final TableDiff.DiffItems... itemsToSkip) {
        assertTableEquals("", expected, actual, itemsToSkip);
    }

    public static void assertEqualsByElements(Table actual, Table expected) {
        final Map<String, ? extends ColumnSource<?>> mapActual = actual.getColumnSourceMap();
        final Map<String, ? extends ColumnSource<?>> mapExpected = expected.getColumnSourceMap();
        Assertions.assertThat(mapActual.keySet()).containsExactlyInAnyOrderElementsOf(mapExpected.keySet());
        for (String key : mapActual.keySet()) {
            final ColumnSource<?> srcActual = mapActual.get(key);
            final ColumnSource<?> srcExpected = mapExpected.get(key);
            // noinspection unchecked,rawtypes
            assertEquals(
                    actual.getRowSet(), (ElementSource) srcActual,
                    expected.getRowSet(), (ElementSource) srcExpected);
        }
    }

    public static <T> void assertEquals(
            RowSet rowsActual, ElementSource<T> srcActual,
            RowSet rowsExpected, ElementSource<T> srcExpected) {
        Assertions.assertThat(rowsActual.size()).isEqualTo(rowsExpected.size());
        try (
                final RowSet.Iterator itActual = rowsActual.iterator();
                final RowSet.Iterator itExpected = rowsExpected.iterator()) {
            while (itActual.hasNext()) {
                if (!itExpected.hasNext()) {
                    throw new IllegalStateException();
                }
                Assertions.assertThat(srcActual.get(itActual.nextLong()))
                        .isEqualTo(srcExpected.get(itExpected.nextLong()));
            }
            if (itExpected.hasNext()) {
                throw new IllegalStateException();
            }
        }
    }

    /**
     * Equivalent to {@code input.getSubTable(RowSetTstUtils.subset(input.getRowSet(), dutyOn, dutyOff).toTracking())}.
     *
     * @param input the input table
     * @param dutyOn the duty-on size
     * @param dutyOff the duty-off size
     * @return a duty-limited subset
     * @see RowSetTstUtils#subset(RowSet, int, int)
     */
    public static Table subset(Table input, int dutyOn, int dutyOff) {
        return input.getSubTable(RowSetTstUtils.subset(input.getRowSet(), dutyOn, dutyOff).toTracking());
    }

    public static void assertTableEquals(final String context, @NotNull final Table expected,
            @NotNull final Table actual, final TableDiff.DiffItems... itemsToSkip) {
        if (itemsToSkip.length > 0) {
            assertTableEquals(context, expected, actual, EnumSet.of(itemsToSkip[0], itemsToSkip));
        } else {
            assertTableEquals(context, expected, actual, EnumSet.noneOf(TableDiff.DiffItems.class));
        }
    }

    public static void assertTableEquals(final String context, @NotNull final Table expected,
            @NotNull final Table actual, final EnumSet<TableDiff.DiffItems> itemsToSkip) {
        final Pair<String, Long> diffPair = TableTools.diffPair(actual, expected, 10, itemsToSkip);
        if (diffPair.getFirst().equals("")) {
            return;
        }
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final long firstRow = Math.max(0, diffPair.getSecond() - 5);
            final long lastRow = Math.max(10, diffPair.getSecond() + 5);

            try (final PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
                TableTools.showWithRowSet(expected, firstRow, lastRow, ps);
            }
            final String expectedString = baos.toString();
            baos.reset();

            try (final PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
                TableTools.showWithRowSet(actual, firstRow, lastRow, ps);
            }
            final String actualString = baos.toString();

            throw new ComparisonFailure(context + "\n" + diffPair.getFirst(), expectedString, actualString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reruns test cases trying new seeds while minimizing the number of steps to catch the failing test. The test
     * should mutate the provided MutableInt so that when it fails it is equal to the current step iteration. This
     * allows the test to minimize the total number of steps per seed as it discovers better candidate parameters.
     *
     * @param test A test instance to tearDown and setUp between each test.
     * @param initialSeed The seed to begin using.
     * @param maxSeed The highest seed to try.
     * @param initialSteps Number of steps to start with.
     * @param runner A method whose first param is the random seed to use, and second parameter is the number of steps.
     */
    public static void findMinimalTestCase(final RefreshingTableTestCase test, final int initialSeed, final int maxSeed,
            final int initialSteps, final BiConsumer<Integer, MutableInt> runner) {
        final boolean origPrintTableUpdates = RefreshingTableTestCase.printTableUpdates;
        RefreshingTableTestCase.printTableUpdates = false;

        int bestSeed = initialSeed;
        int bestSteps = initialSteps;
        boolean failed = false;
        MutableInt maxSteps = new MutableInt(initialSteps);
        for (int seed = initialSeed; seed < maxSeed; ++seed) {
            if (maxSteps.get() <= 0) {
                System.out.println("Best Run: bestSeed=" + bestSeed + " bestSteps=" + bestSteps);
                return;
            }
            System.out.println("Running: seed=" + seed + " numSteps=" + maxSteps.get() + " bestSeed=" + bestSeed
                    + " bestSteps=" + bestSteps);
            if (seed != initialSeed) {
                try {
                    test.tearDown();
                } catch (Exception | Error e) {
                    System.out.println("Error on test.tearDown():");
                    e.printStackTrace();
                }
                try {
                    test.setUp();
                } catch (Exception | Error e) {
                    System.out.println("Error on test.setUp():");
                    e.printStackTrace();
                }
            }
            try {
                runner.accept(seed, maxSteps);
            } catch (Exception | Error e) {
                failed = true;
                bestSeed = seed;
                bestSteps = maxSteps.get() + 1;
                e.printStackTrace();
                System.out.println("Candidate: seed=" + seed + " numSteps=" + (maxSteps.get() + 1));
            }
        }

        RefreshingTableTestCase.printTableUpdates = origPrintTableUpdates;
        if (failed) {
            throw new RuntimeException("Debug candidate: seed=" + bestSeed + " steps=" + bestSteps);
        }
    }

    public static void expectLivenessException(@NotNull final Runnable action) {
        try {
            action.run();
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted("Expected LivenessStateException");
        } catch (LivenessStateException ignored) {
        }
    }

    public static void assertThrows(final Runnable runnable) {
        boolean threwException = false;
        try {
            runnable.run();
        } catch (final Exception ignored) {
            threwException = true;
        }
        TestCase.assertTrue(threwException);
    }

    public static void tableRangesAreEqual(Table table1, Table table2, long from1, long from2, long size) {
        assertTableEquals(table1.tail(table1.size() - from1).head(size),
                table2.tail(table2.size() - from2).head(size));
    }

    /**
     * Make a copy of {@code table} with a new RowSet that introduces sparsity by multiplying each row key by
     * {@code sparsityFactor}.
     * 
     * @param table The Table to make a sparse copy of (must be static)
     * @param sparsityFactor The sparsity factor to apply
     * @return A sparse copy of {@code table}
     */
    public static Table sparsify(@NotNull final Table table, final long sparsityFactor) {
        // Only static support for now. For refreshing support, add a listener to propagate expanded TableUpdates.
        Assert.assertion(!table.isRefreshing(), "!table.isRefreshing()");

        final WritableRowSet outputRowSet;
        {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final RowSet inputRowSet = table.getRowSet();
            // Using multiplyExact here allows us to throw an ArithmeticException if we'll overflow
            final long expectedLastRowKey = Math.multiplyExact(inputRowSet.lastRowKey(), sparsityFactor);
            inputRowSet.forAllRowKeys((final long rowKey) -> builder.appendKey(rowKey * sparsityFactor));
            // noinspection resource
            outputRowSet = builder.build();
            Assert.eq(expectedLastRowKey, "expectedLastRowKey", outputRowSet.lastRowKey(), "outputRowSet.lastRowKey()");
        }
        final Map<String, ? extends ColumnSource<?>> outputColumnSources;
        {
            final RowRedirection densifyingRedirection = new LongColumnSourceRowRedirection<>(
                    new ViewColumnSource<>(Long.class, new DensifyRowKeysFormula(sparsityFactor), true));
            outputColumnSources = table.getColumnSourceMap().entrySet().stream().collect(Collectors.toMap(
                    (Function<Map.Entry<String, ? extends ColumnSource<?>>, String>) Map.Entry::getKey,
                    (final Map.Entry<String, ? extends ColumnSource<?>> entry) -> RedirectedColumnSource
                            .maybeRedirect(densifyingRedirection, entry.getValue()),
                    Assert::neverInvoked,
                    LinkedHashMap::new));
        }
        return new QueryTable(outputRowSet.toTracking(), outputColumnSources);
    }

    private static final class DensifyRowKeysFormula extends Formula {

        private static final FillContext FILL_CONTEXT = new FillContext() {};

        private final long sparsityFactor;

        private DensifyRowKeysFormula(final long sparsityFactor) {
            super(null);
            this.sparsityFactor = sparsityFactor;
        }

        private long densify(final long rowKey) {
            Assert.eqZero(rowKey % sparsityFactor, "rowKey % sparsityFactor");
            return rowKey / sparsityFactor;
        }

        @Override
        public Long get(final long rowKey) {
            return TypeUtils.box(densify(rowKey));
        }

        @Override
        public Long getPrev(final long rowKey) {
            return get(rowKey);
        }

        @Override
        public long getLong(long rowKey) {
            return densify(rowKey);
        }

        @Override
        public long getPrevLong(long rowKey) {
            return getLong(rowKey);
        }

        @Override
        protected ChunkType getChunkType() {
            return ChunkType.Long;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity) {
            return FILL_CONTEXT;
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            destination.setSize(0);
            final WritableLongChunk<? super Values> typedDestination = destination.asWritableLongChunk();
            rowSequence.forAllRowKeys((final long rowKey) -> typedDestination.add(getLong(rowKey)));
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            fillChunk(context, destination, rowSequence);
        }
    }

    /**
     * Fetch row data as boxed Objects for the specified row position and column names. This is not an efficient
     * data-retrieval mechanism, just a convenient one.
     *
     * @param table The table to fetch from
     * @param rowPosition The row position to fetch
     * @param columnNames The names of columns to fetch; if empty, fetch all columns
     * @return An array of row data as boxed Objects
     */
    public static Object[] getRowData(
            @NotNull Table table,
            final long rowPosition,
            @NotNull final String... columnNames) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            table = table.coalesce();
            final long rowKey = table.getRowSet().get(rowPosition);
            return (columnNames.length > 0
                    ? Arrays.stream(columnNames).map(table::getColumnSource)
                    : table.getColumnSources().stream())
                    .map(columnSource -> columnSource.get(rowKey))
                    .toArray(Object[]::new);
        }
    }
}
