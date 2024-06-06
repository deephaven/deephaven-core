//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.sources.InstantSparseArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ObjectSparseArraySource;
import io.deephaven.engine.table.impl.sources.ZonedDateTimeArraySource;
import io.deephaven.engine.table.impl.sources.ZonedDateTimeSparseArraySource;
import io.deephaven.engine.table.impl.util.TableTimeConversions;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class TestReinterpretedColumn extends RefreshingTableTestCase {
    final int ROW_COUNT = 60;
    private final long baseLongTime = DateTimeUtils.parseEpochNanos("2021-10-20T09:30:00.000 NY");
    private final ZonedDateTime baseZDT = ZonedDateTime.of(2021, 10, 18, 11, 30, 0, 0, ZoneId.of("America/New_York"));
    private final Instant baseInstant = DateTimeUtils.parseInstant("2021-10-17T12:30:00.000 NY");

    private QueryTable baseTable;
    private QueryTable sparseBaseTable;
    private QueryTable objectTable;
    private QueryTable sparseObjectTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        baseTable = makeTable(
                new LongArraySource(),
                new InstantArraySource(),
                new ZonedDateTimeArraySource(ZoneId.of("America/New_York")));

        sparseBaseTable = makeTable(
                new LongSparseArraySource(),
                new InstantSparseArraySource(),
                new ZonedDateTimeSparseArraySource(ZoneId.of("America/New_York")));

        objectTable = makeObjectTable(
                new LongArraySource(),
                new ObjectArraySource<>(Instant.class),
                new ObjectArraySource<>(ZonedDateTime.class));

        sparseObjectTable = makeObjectTable(
                new LongSparseArraySource(),
                new ObjectSparseArraySource<>(Instant.class),
                new ObjectSparseArraySource<>(ZonedDateTime.class));
    }

    private QueryTable makeObjectTable(
            WritableColumnSource<Long> longSource,
            WritableColumnSource<Instant> iSource,
            WritableColumnSource<ZonedDateTime> zdtSource) {
        longSource.ensureCapacity(ROW_COUNT);
        iSource.ensureCapacity(ROW_COUNT);
        zdtSource.ensureCapacity(ROW_COUNT);

        for (int ii = 0; ii < ROW_COUNT; ii++) {
            final long tOff = ii * 60 * 1_000_000_000L;
            longSource.set(ii, Long.valueOf(baseLongTime + tOff));
            iSource.set(ii, DateTimeUtils.epochNanosToInstant(DateTimeUtils.epochNanos(baseInstant) + tOff));
            zdtSource.set(ii, DateTimeUtils.epochNanosToZonedDateTime(DateTimeUtils.epochNanos(baseZDT) + tOff,
                    ZoneId.of("America/New_York")));
        }

        final Map<String, ColumnSource<?>> cols = new LinkedHashMap<>();
        cols.put("L", longSource);
        cols.put("I", iSource);
        cols.put("ZDT", zdtSource);

        cols.values().forEach(ColumnSource::startTrackingPrevValues);

        return new QueryTable(RowSetFactory.flat(ROW_COUNT).toTracking(), cols);
    }

    private QueryTable makeTable(
            WritableColumnSource<Long> longSource,
            WritableColumnSource<Instant> iSource,
            WritableColumnSource<ZonedDateTime> zdtSource) {
        longSource.ensureCapacity(ROW_COUNT);
        iSource.ensureCapacity(ROW_COUNT);
        zdtSource.ensureCapacity(ROW_COUNT);

        for (int ii = 0; ii < ROW_COUNT; ii++) {
            final long tOff = ii * 60 * 1_000_000_000L;
            longSource.set(ii, baseLongTime + tOff);
            iSource.set(ii, DateTimeUtils.epochNanos(baseInstant) + tOff);
            zdtSource.set(ii, DateTimeUtils.epochNanos(baseZDT) + tOff);
        }

        final Map<String, ColumnSource<?>> cols = new LinkedHashMap<>();
        cols.put("L", longSource);
        cols.put("I", iSource);
        cols.put("ZDT", zdtSource);

        cols.values().forEach(ColumnSource::startTrackingPrevValues);

        return new QueryTable(RowSetFactory.flat(ROW_COUNT).toTracking(), cols);
    }

    private long computeTimeDiff(final int iteration, boolean invert) {
        return (invert ? ROW_COUNT - iteration - 1 : iteration) * 60 * 1_000_000_000L;
    }

    @Test
    public void testReinterpretLong() {
        testReinterpretLong(baseTable, false, false);
        testReinterpretLong(baseTable, false, true);
        testReinterpretLong(sparseBaseTable, false, false);
        testReinterpretLong(sparseBaseTable, false, true);
        testReinterpretLong(objectTable, false, false);
        testReinterpretLong(objectTable, false, true);
        testReinterpretLong(sparseObjectTable, false, false);
        testReinterpretLong(sparseObjectTable, false, true);
    }

    private void testReinterpretLong(final Table initial, boolean isSorted, boolean withRename) {
        final String lColName = withRename ? "R_L" : "L";
        final String iColName = withRename ? "R_I" : "I";
        final String zdtColName = withRename ? "R_ZDT" : "ZDT";

        // Make everything a long
        Table table = TableTimeConversions.asEpochNanos(initial, lColName + "=L");
        table = TableTimeConversions.asEpochNanos(table, iColName + "=I");
        table = TableTimeConversions.asEpochNanos(table, zdtColName + "=ZDT");

        TableDefinition td = table.getDefinition();
        assertEquals(long.class, td.getColumn(lColName).getDataType());
        if (!withRename) {
            assertEquals(initial.getColumnSource("L"), table.getColumnSource(lColName));
        }
        assertEquals(long.class, td.getColumn(iColName).getDataType());
        assertEquals(long.class, td.getColumn(zdtColName).getDataType());

        final MutableInt ii = new MutableInt(0);
        for (final RowSet.Iterator it = table.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            final long tOff = computeTimeDiff(ii.getAndIncrement(), isSorted);
            if (table.getColumnSource(lColName) instanceof ObjectArraySource
                    || table.getColumnSource(lColName) instanceof ObjectSparseArraySource) {
                assertEquals(baseLongTime + tOff, table.getColumnSource(lColName).get(key));
            } else {
                assertEquals(baseLongTime + tOff, table.getColumnSource(lColName).getLong(key));
            }
            assertEquals(DateTimeUtils.epochNanos(baseInstant) + tOff, table.getColumnSource(iColName).getLong(key));
            assertEquals(DateTimeUtils.epochNanos(baseZDT) + tOff, table.getColumnSource(zdtColName).getLong(key));
        }

        // Repeat the same comparisons, but actuate getChunk instead
        reinterpLongChunkCheck(table.getColumnSource(lColName), table.getRowSet(), isSorted, baseLongTime);
        reinterpLongChunkCheck(table.getColumnSource(iColName), table.getRowSet(), isSorted,
                DateTimeUtils.epochNanos(baseInstant));
        reinterpLongChunkCheck(table.getColumnSource(zdtColName), table.getRowSet(), isSorted,
                DateTimeUtils.epochNanos(baseZDT));

        if (!isSorted) {
            testReinterpretLong(initial.sortDescending("L"), true, withRename);
        }
    }

    private void reinterpLongChunkCheck(final ColumnSource<Long> cs, RowSet rowSet, final boolean isSorted,
            final long baseNanos) {
        try (final ChunkSource.GetContext gc = cs.makeGetContext(64)) {
            for (final boolean usePrev : new boolean[] {false, true}) {
                final LongChunk<? extends Values> chunk = usePrev
                        ? cs.getPrevChunk(gc, rowSet).asLongChunk()
                        : cs.getChunk(gc, rowSet).asLongChunk();

                for (int ii = 0; ii < chunk.size(); ii++) {
                    final long tOff = computeTimeDiff(ii, isSorted);
                    assertEquals(baseNanos + tOff, chunk.get(ii));
                }
            }
        }
    }

    private <T> void doReinterpretTestBasic(final Table initial,
            final Class<T> expectedType,
            final BiFunction<Table, String, Table> reinterpreter,
            String equalColumn,
            Function<T, Long> toNanoFunc) {
        doReinterpretTestBasic(initial, expectedType, reinterpreter, equalColumn, toNanoFunc, false, t -> {
        }, false);
    }

    private <T> void doReinterpretTestBasic(final Table initial,
            final Class<T> expectedType,
            final BiFunction<Table, String, Table> reinterpreter,
            String equalColumn,
            Function<T, Long> toNanoFunc,
            Consumer<T> extraCheck) {
        doReinterpretTestBasic(initial, expectedType, reinterpreter, equalColumn, toNanoFunc, false, extraCheck, false);
    }

    @SuppressWarnings("unchecked")
    private <T> void doReinterpretTestBasic(final Table initial,
            final Class<T> expectedType,
            final BiFunction<Table, String, Table> reinterpreter,
            String equalColumn,
            Function<T, Long> toNanoFunc,
            boolean isSorted,
            Consumer<T> extraCheck,
            boolean withRename) {
        final String lColName = withRename ? "R_L" : "L";
        final String iColName = withRename ? "R_I" : "I";
        final String zdtColName = withRename ? "R_ZDT" : "ZDT";

        // Make everything the expected type
        Table table = reinterpreter.apply(initial, lColName + "=L");
        table = reinterpreter.apply(table, iColName + "=I");
        table = reinterpreter.apply(table, zdtColName + "=ZDT");

        TableDefinition td = table.getDefinition();
        assertEquals(expectedType, td.getColumn(lColName).getDataType());
        assertEquals(expectedType, td.getColumn(iColName).getDataType());
        assertEquals(expectedType, td.getColumn(zdtColName).getDataType());

        if (equalColumn != null && !withRename) {
            assertEquals(initial.getColumnSource(equalColumn), table.getColumnSource(equalColumn));
        }

        final MutableInt ii = new MutableInt(0);
        for (final RowSet.Iterator it = table.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            final long tOff = computeTimeDiff(ii.getAndIncrement(), isSorted);
            assertEquals(baseLongTime + tOff,
                    (long) toNanoFunc.apply((T) table.getColumnSource(lColName).get(key)));
            extraCheck.accept((T) table.getColumnSource(lColName).get(key));
            assertEquals(DateTimeUtils.epochNanos(baseInstant) + tOff,
                    (long) toNanoFunc.apply((T) table.getColumnSource(iColName).get(key)));
            extraCheck.accept((T) table.getColumnSource(iColName).get(key));
            assertEquals(DateTimeUtils.epochNanos(baseZDT) + tOff,
                    (long) toNanoFunc.apply((T) table.getColumnSource(zdtColName).get(key)));
            extraCheck.accept((T) table.getColumnSource(zdtColName).get(key));
        }

        // Repeat the same comparisons, but actuate getChunk instead
        reinterpBasicChunkCheck(table.getColumnSource(lColName), table.getRowSet(), toNanoFunc, isSorted,
                baseLongTime, extraCheck);
        reinterpBasicChunkCheck(table.getColumnSource(iColName), table.getRowSet(), toNanoFunc, isSorted,
                DateTimeUtils.epochNanos(baseInstant), extraCheck);
        reinterpBasicChunkCheck(table.getColumnSource(zdtColName), table.getRowSet(), toNanoFunc, isSorted,
                DateTimeUtils.epochNanos(baseZDT), extraCheck);

        if (!isSorted) {
            doReinterpretTestBasic(initial.sortDescending("L"), expectedType, reinterpreter, equalColumn, toNanoFunc,
                    true, extraCheck, withRename);
        }
    }

    private <T> void reinterpBasicChunkCheck(final ColumnSource<T> cs, final RowSet rowSet,
            final Function<T, Long> toNanoFunc, final boolean isSorted, final long baseNanos,
            final Consumer<T> extraCheck) {
        try (final ChunkSource.GetContext gc = cs.makeGetContext(64)) {
            for (final boolean usePrev : new boolean[] {false, true}) {
                final ObjectChunk<T, ? extends Values> chunk = usePrev
                        ? cs.getPrevChunk(gc, rowSet).asObjectChunk()
                        : cs.getChunk(gc, rowSet).asObjectChunk();

                for (int ii = 0; ii < chunk.size(); ii++) {
                    final long tOff = computeTimeDiff(ii, isSorted);
                    assertEquals(baseNanos + tOff, (long) toNanoFunc.apply(chunk.get(ii)));
                    extraCheck.accept(chunk.get(ii));
                }
            }
        }
    }

    @Test
    public void testReinterpretInstant() {
        doReinterpretTestBasic(
                baseTable, Instant.class, TableTimeConversions::asInstant, "I", DateTimeUtils::epochNanos);
        doReinterpretTestBasic(
                sparseBaseTable, Instant.class, TableTimeConversions::asInstant, "I", DateTimeUtils::epochNanos);
        doReinterpretTestBasic(
                objectTable, Instant.class, TableTimeConversions::asInstant, "I", DateTimeUtils::epochNanos);
        doReinterpretTestBasic(
                sparseObjectTable, Instant.class, TableTimeConversions::asInstant, "I", DateTimeUtils::epochNanos);
    }

    @Test
    public void testReinterpretZdt() {
        final Consumer<ZonedDateTime> extraCheck =
                zdt -> assertTrue(zdt == null || zdt.getZone().equals(ZoneId.of("America/Chicago")));

        doReinterpretTestBasic(baseTable, ZonedDateTime.class,
                (t, c) -> TableTimeConversions.asZonedDateTime(t, c, "America/Chicago"),
                null, DateTimeUtils::epochNanos, extraCheck);
        doReinterpretTestBasic(sparseBaseTable, ZonedDateTime.class,
                (t, c) -> TableTimeConversions.asZonedDateTime(t, c, "America/Chicago"),
                null, DateTimeUtils::epochNanos, extraCheck);
        doReinterpretTestBasic(objectTable, ZonedDateTime.class,
                (t, c) -> TableTimeConversions.asZonedDateTime(t, c, "America/Chicago"),
                null, DateTimeUtils::epochNanos, extraCheck);
        doReinterpretTestBasic(sparseObjectTable, ZonedDateTime.class,
                (t, c) -> TableTimeConversions.asZonedDateTime(t, c, "America/Chicago"),
                null, DateTimeUtils::epochNanos, extraCheck);
    }

    private <T> void reinterpWrappedChunkCheck(final ColumnSource<T> cs, RowSet rowSet, final boolean isSorted,
            final BiFunction<Integer, Boolean, T> expectedSupplier) {
        try (final ChunkSource.GetContext gc = cs.makeGetContext(64)) {
            for (final boolean usePrev : new boolean[] {false, true}) {
                final ObjectChunk<T, ? extends Values> chunk = usePrev
                        ? cs.getPrevChunk(gc, rowSet).asObjectChunk()
                        : cs.getChunk(gc, rowSet).asObjectChunk();

                for (int ii = 0; ii < chunk.size(); ii++) {
                    assertEquals(expectedSupplier.apply(ii, isSorted), chunk.get(ii));
                }
            }
        }
    }

    @Test
    public void testReinterpretLocalDate() {
        doTestReinterpretLocalDate(baseTable, false);
        doTestReinterpretLocalDate(sparseBaseTable, false);
        doTestReinterpretLocalDate(objectTable, false);
        doTestReinterpretLocalDate(sparseObjectTable, false);
    }

    private void doTestReinterpretLocalDate(final Table initial, boolean sorted) {
        Table table = TableTimeConversions.asLocalDate(initial, "L", "America/Chicago");
        table = TableTimeConversions.asLocalDate(table, "I", "America/Chicago");
        table = TableTimeConversions.asLocalDate(table, "ZDT", "America/Chicago");

        TableDefinition td = table.getDefinition();
        assertEquals(LocalDate.class, td.getColumn("L").getDataType());
        assertEquals(LocalDate.class, td.getColumn("I").getDataType());
        assertEquals(LocalDate.class, td.getColumn("ZDT").getDataType());

        for (final RowSet.Iterator it = table.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            assertEquals(LocalDate.of(2021, 10, 20), table.getColumnSource("L").get(key));
            assertEquals(LocalDate.of(2021, 10, 17), table.getColumnSource("I").get(key));
            assertEquals(LocalDate.of(2021, 10, 18), table.getColumnSource("ZDT").get(key));
        }

        reinterpWrappedChunkCheck(
                table.getColumnSource("L"), table.getRowSet(), sorted, (i, s) -> LocalDate.of(2021, 10, 20));
        reinterpWrappedChunkCheck(
                table.getColumnSource("I"), table.getRowSet(), sorted, (i, s) -> LocalDate.of(2021, 10, 17));
        reinterpWrappedChunkCheck(
                table.getColumnSource("ZDT"), table.getRowSet(), sorted, (i, s) -> LocalDate.of(2021, 10, 18));
        if (!sorted) {
            doTestReinterpretLocalDate(initial.sortDescending("L"), true);
        }
    }

    @Test
    public void testReinterpretLocalTime() {
        doTestReinterpretLocalTime(baseTable, false);
        doTestReinterpretLocalTime(sparseBaseTable, false);
        doTestReinterpretLocalTime(objectTable, false);
        doTestReinterpretLocalTime(sparseObjectTable, false);
    }

    private void doTestReinterpretLocalTime(final Table initial, boolean sorted) {
        Table table = TableTimeConversions.asLocalTime(initial, "L", "America/Chicago");
        table = TableTimeConversions.asLocalTime(table, "I", "America/Chicago");
        table = TableTimeConversions.asLocalTime(table, "ZDT", "America/Chicago");

        TableDefinition td = table.getDefinition();
        assertEquals(LocalTime.class, td.getColumn("L").getDataType());
        assertEquals(LocalTime.class, td.getColumn("I").getDataType());
        assertEquals(LocalTime.class, td.getColumn("ZDT").getDataType());

        final MutableInt ii = new MutableInt(0);
        for (final RowSet.Iterator it = table.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            final int localII = ii.getAndIncrement();
            final int startIter = sorted ? ROW_COUNT - localII - 1 : localII;
            final int hourOff = startIter / 30;
            final int minute = (startIter + 30) % 60;
            assertEquals(LocalTime.of(8 + hourOff, minute, 0), table.getColumnSource("L").get(key));
            assertEquals(LocalTime.of(11 + hourOff, minute, 0), table.getColumnSource("I").get(key));
            assertEquals(LocalTime.of(10 + hourOff, minute, 0), table.getColumnSource("ZDT").get(key));
        }

        reinterpWrappedChunkCheck(
                table.getColumnSource("L"), table.getRowSet(), sorted, (i, s) -> makeLocalTime(8, i, s));
        reinterpWrappedChunkCheck(
                table.getColumnSource("I"), table.getRowSet(), sorted, (i, s) -> makeLocalTime(11, i, s));
        reinterpWrappedChunkCheck(
                table.getColumnSource("ZDT"), table.getRowSet(), sorted, (i, s) -> makeLocalTime(10, i, s));

        if (!sorted) {
            doTestReinterpretLocalTime(initial.sortDescending("L"), true);
        }
    }

    private LocalTime makeLocalTime(int hour, int ii, boolean sorted) {
        final int startIter = sorted ? ROW_COUNT - ii - 1 : ii;
        final int hourOff = startIter / 30;
        final int minute = (startIter + 30) % 60;

        return LocalTime.of(hour + hourOff, minute, 0);
    }

    @Test
    public void testFillPrevOnInstantColumn() {
        final InstantArraySource source = new InstantArraySource();
        source.startTrackingPrevValues();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        try (final RowSet latticeRows = RowSetFactory.flat(1024);
                final RowSet unchangedRows = RowSetFactory.flat(1024).shift(ArrayBackedColumnSource.BLOCK_SIZE)) {
            source.ensureCapacity(unchangedRows.lastRowKey() + 1);
            updateGraph.runWithinUnitTestCycle(() -> {
                latticeRows.forAllRowKeys(row -> {
                    source.set(row, Instant.ofEpochMilli(row));
                });

                // create rows that won't change in a separate block for code coverage
                unchangedRows.forAllRowKeys(row -> {
                    source.set(row, Instant.ofEpochMilli(row));
                });
            });

            updateGraph.runWithinUnitTestCycle(() -> {
                // change only some rows; for coverage
                latticeRows.forAllRowKeys(row -> {
                    if (row % 2 == 0) {
                        source.set(row, Instant.ofEpochMilli(row + latticeRows.size()));
                    }
                });

                try (final RowSet allRows = latticeRows.union(unchangedRows);
                        final ChunkSource.FillContext context = source.makeFillContext(allRows.intSize());
                        final WritableObjectChunk<Instant, Values> chunk =
                                WritableObjectChunk.makeWritableChunk(allRows.intSize())) {
                    source.fillPrevChunk(context, chunk, allRows);
                    allRows.forAllRowKeys(row -> {
                        final long chunkOffset = row + (row < ArrayBackedColumnSource.BLOCK_SIZE
                                ? 0
                                : (latticeRows.size() - ArrayBackedColumnSource.BLOCK_SIZE));
                        assertEquals(Instant.ofEpochMilli(row), chunk.get((int) chunkOffset));
                    });
                }
            });
        }
    }
}
