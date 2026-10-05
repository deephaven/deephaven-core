//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A {@code whereIn} or {@code whereNotIn} of a large static table against a ticking set table.
 *
 * <p>
 * There are {@code keys} distinct key ids. Source row {@code ii} holds key id {@code (ii * STRIDE) % keys}, so that the
 * rows matching any one id are scattered across the source. The set table holds {@code setKeys} consecutive ids from a
 * window that slides by {@code churn} ids each cycle: set row key {@code s} holds id {@code s % keys}, the window's
 * oldest {@code churn} rows are removed and {@code churn} new rows are added after its newest. Every cycle therefore
 * both adds and removes set keys, which re-filters both the matched and the unmatched rows of the source.
 * </p>
 *
 * <p>
 * The {@link KeyShape} parameter selects the key columns. The single column shapes ({@code LONG}, {@code STRING})
 * filter on {@code Key1}; the multiple column shapes ({@code LONG_INT}, {@code STRING_LONG}) filter on {@code Key1} and
 * {@code Key2}, which together identify an id while neither column alone does.
 * </p>
 *
 * <p>
 * {@link #initial} measures creating the filtered result from scratch, including building the set; {@link #setCycle}
 * measures one update cycle of the set table. Run with {@code -prof gc} for allocation ({@code gc.alloc.rate.norm} is
 * bytes per operation) and collection counts and times; an allocation profile by class, from which object counts may be
 * estimated, is available with {@code -prof async:event=alloc}.
 * </p>
 *
 * <pre>
 * ./gradlew engine-table:jmhJar
 * java -jar engine/table/build/libs/deephaven-engine-table-&lt;version&gt;-jmh.jar WhereInBenchmark -prof gc
 * </pre>
 */
@Fork(value = 2, jvmArgs = {"-Xms8G", "-Xmx8G"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class WhereInBenchmark {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    /**
     * A prime, so that consecutive source rows hold ids far apart.
     */
    private static final long STRIDE = 1_000_003L;

    /**
     * The number of distinct {@code Key2} values in the multiple column shapes.
     */
    private static final int KEY2_CARDINALITY = 16;

    public enum KeyShape {
        LONG(false), STRING(false), LONG_INT(true), STRING_LONG(true);

        private final boolean multipleColumns;

        KeyShape(final boolean multipleColumns) {
            this.multipleColumns = multipleColumns;
        }
    }

    @Param({"LONG", "LONG_INT"})
    public KeyShape keyShape;

    @Param({"true", "false"})
    public boolean inclusion;

    @Param({"10000000"})
    public int rows;

    @Param({"1000000"})
    public int keys;

    @Param({"500000"})
    public int setKeys;

    @Param({"1000"})
    public int churn;

    private EngineCleanup engine;
    private ControlledUpdateGraph updateGraph;
    private LivenessScope scope;

    /** Key values by id. */
    private long[] longKeys;
    private int[] intKeys;
    private String[] stringKeys;

    private Table source;
    private TrackingWritableRowSet setRowSet;
    private QueryTable setTable;
    private String[] matchColumns;

    private Table result;
    private BlackholeListener listener;

    @Setup(Level.Trial)
    public void setupTrial(final Blackhole blackhole) throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Key1 alone repeats across KEY2_CARDINALITY ids in the multiple column shapes.
        final int key1Divisor = keyShape.multipleColumns ? KEY2_CARDINALITY : 1;
        longKeys = new long[keys];
        intKeys = new int[keys];
        stringKeys = new String[keys];
        for (int id = 0; id < keys; ++id) {
            longKeys[id] = id / key1Divisor;
            intKeys[id] = id % KEY2_CARDINALITY;
            stringKeys[id] = "Key-" + (id / key1Divisor);
        }

        scope = new LivenessScope();
        LivenessScopeStack.push(scope);

        source = makeSource();
        matchColumns = keyShape.multipleColumns ? new String[] {"Key1", "Key2"} : new String[] {"Key1"};

        setRowSet = RowSetFactory.fromRange(0, setKeys - 1).toTracking();
        final Map<String, ColumnSource<?>> setColumns = new LinkedHashMap<>();
        switch (keyShape) {
            case LONG:
                setColumns.put("Key1", new LongKeySource(longKeys));
                break;
            case STRING:
                setColumns.put("Key1", new StringKeySource(stringKeys));
                break;
            case LONG_INT:
                setColumns.put("Key1", new LongKeySource(longKeys));
                setColumns.put("Key2", new IntKeySource(intKeys));
                break;
            case STRING_LONG:
                setColumns.put("Key1", new StringKeySource(stringKeys));
                final long[] key2 = new long[keys];
                for (int id = 0; id < keys; ++id) {
                    key2[id] = intKeys[id];
                }
                setColumns.put("Key2", new LongKeySource(key2));
                break;
        }
        setTable = new QueryTable(setRowSet, setColumns);
        setTable.setRefreshing(true);

        result = filter();
        listener = new BlackholeListener(blackhole);
        result.addUpdateListener(listener);
    }

    private Table makeSource() {
        final long[] sourceLong = new long[rows];
        final int[] sourceInt = new int[rows];
        final String[] sourceString = new String[rows];
        for (int ii = 0; ii < rows; ++ii) {
            final int id = (int) ((ii * STRIDE) % keys);
            sourceLong[ii] = longKeys[id];
            sourceInt[ii] = intKeys[id];
            sourceString[ii] = stringKeys[id];
        }
        switch (keyShape) {
            case LONG:
                return TableTools.newTable(TableTools.longCol("Key1", sourceLong));
            case STRING:
                return TableTools.newTable(TableTools.stringCol("Key1", sourceString));
            case LONG_INT:
                return TableTools.newTable(
                        TableTools.longCol("Key1", sourceLong),
                        TableTools.intCol("Key2", sourceInt));
            case STRING_LONG:
                // Key2 must distinguish the ids that share Key1, so use the int key widened to long.
                final long[] sourceKey2 = new long[rows];
                for (int ii = 0; ii < rows; ++ii) {
                    sourceKey2[ii] = sourceInt[ii];
                }
                return TableTools.newTable(
                        TableTools.stringCol("Key1", sourceString),
                        TableTools.longCol("Key2", sourceKey2));
        }
        throw new IllegalStateException();
    }

    private Table filter() {
        return inclusion ? source.whereIn(setTable, matchColumns) : source.whereNotIn(setTable, matchColumns);
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws Exception {
        result.removeUpdateListener(listener);
        listener = null;
        result = null;
        setTable = null;
        setRowSet = null;
        source = null;
        LivenessScopeStack.pop(scope);
        scope.release();
        scope = null;
        updateGraph = null;
        engine.tearDown();
        engine = null;
    }

    /**
     * Create the filtered result from scratch, building the set from the set table as it stands.
     */
    @Benchmark
    public long initial() {
        final LivenessScope invocationScope = new LivenessScope();
        LivenessScopeStack.push(invocationScope);
        try {
            return filter().size();
        } finally {
            LivenessScopeStack.pop(invocationScope);
            invocationScope.release();
        }
    }

    /**
     * Slide the set window by {@code churn} keys and propagate the change through the filtered result.
     */
    @Benchmark
    public long setCycle() throws Throwable {
        updateGraph.runWithinUnitTestCycle(this::slideSet);
        if (listener.e != null) {
            throw listener.e;
        }
        return result.size();
    }

    private void slideSet() {
        final long first = setRowSet.firstRowKey();
        final long last = setRowSet.lastRowKey();
        final WritableRowSet removed = RowSetFactory.fromRange(first, first + churn - 1);
        final WritableRowSet added = RowSetFactory.fromRange(last + 1, last + churn);
        setRowSet.remove(removed);
        setRowSet.insert(added);
        setTable.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(),
                RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
    }

    /**
     * A set table column holding the key of id {@code rowKey % keys}; a row's value never changes, so the previous
     * value is the current one.
     */
    private static class LongKeySource extends AbstractColumnSource<Long>
            implements MutableColumnSourceGetDefaults.ForLong {
        private final long[] values;

        private LongKeySource(final long[] values) {
            super(long.class);
            this.values = values;
        }

        @Override
        public long getLong(final long rowKey) {
            return values[(int) (rowKey % values.length)];
        }

        @Override
        public long getPrevLong(final long rowKey) {
            return getLong(rowKey);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            final WritableLongChunk<? super Values> longDest = dest.asWritableLongChunk();
            longDest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> longDest.add(values[(int) (rowKey % values.length)]));
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest, @NotNull final RowSequence rowSequence) {
            fillChunk(context, dest, rowSequence);
        }
    }

    /**
     * An int counterpart of {@link LongKeySource}.
     */
    private static class IntKeySource extends AbstractColumnSource<Integer>
            implements MutableColumnSourceGetDefaults.ForInt {
        private final int[] values;

        private IntKeySource(final int[] values) {
            super(int.class);
            this.values = values;
        }

        @Override
        public int getInt(final long rowKey) {
            return values[(int) (rowKey % values.length)];
        }

        @Override
        public int getPrevInt(final long rowKey) {
            return getInt(rowKey);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            final WritableIntChunk<? super Values> intDest = dest.asWritableIntChunk();
            intDest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> intDest.add(values[(int) (rowKey % values.length)]));
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest, @NotNull final RowSequence rowSequence) {
            fillChunk(context, dest, rowSequence);
        }
    }

    /**
     * A String counterpart of {@link LongKeySource}, returning the same String instances as the source table holds.
     */
    private static class StringKeySource extends AbstractColumnSource<String>
            implements MutableColumnSourceGetDefaults.ForObject<String> {
        private final String[] values;

        private StringKeySource(final String[] values) {
            super(String.class);
            this.values = values;
        }

        @Override
        public String get(final long rowKey) {
            return values[(int) (rowKey % values.length)];
        }

        @Override
        public String getPrev(final long rowKey) {
            return get(rowKey);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> dest,
                @NotNull final RowSequence rowSequence) {
            final WritableObjectChunk<String, ? super Values> objectDest = dest.asWritableObjectChunk();
            objectDest.setSize(0);
            rowSequence.forAllRowKeys(rowKey -> objectDest.add(values[(int) (rowKey % values.length)]));
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> dest, @NotNull final RowSequence rowSequence) {
            fillChunk(context, dest, rowSequence);
        }
    }
}
