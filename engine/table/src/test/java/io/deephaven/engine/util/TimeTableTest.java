//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.ReinterpretedColumn;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.iterators.ChunkedLongColumnIterator;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TimeTableTest extends RefreshingTableTestCase {

    private TestClock clock;
    private UpdateSourceCombiner updateSourceCombiner;
    private QueryTable timeTable;
    private TableUpdateValidator validator;
    private ColumnSource<Long> column;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        clock = new TestClock(0);
        updateSourceCombiner = new UpdateSourceCombiner(ExecutionContext.getContext().getUpdateGraph());
    }

    @Override
    public void tearDown() throws Exception {
        validator.deepValidation();

        clock = null;
        updateSourceCombiner = null;
        timeTable = null;
        validator = null;
        column = null;

        super.tearDown();
    }

    private void build(TimeTable.Builder builder) {
        timeTable = builder
                .registrar(updateSourceCombiner)
                .clock(clock)
                .build();
        column = timeTable.getColumnSource("Timestamp").reinterpret(long.class);
        validator = TableUpdateValidator.make(timeTable);
    }

    private void tick(long tm) {
        clock.now = tm;
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        validator.validate();
    }

    @Test
    public void testNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().period(10));

        tick(0);
        Assert.assertEquals(timeTable.size(), 1L);
        Assert.assertEquals(column.getLong(0), 0);

        tick(9);
        Assert.assertEquals(timeTable.size(), 1);

        tick(10);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 10);

        // Check that it will tick multiple rows.
        tick(100);
        Assert.assertEquals(timeTable.size(), 11);
    }

    @Test
    public void testNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().period(10));

        tick(15);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);

        tick(24);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 20);
    }

    @Test
    public void testProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(10))
                .period(10));

        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        tick(10);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);
    }

    @Test
    public void testProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(15))
                .period(10));

        tick(14);
        Assert.assertEquals(timeTable.size(), 0);

        tick(16);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 15);

        tick(34);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 25);
    }

    @Test
    public void testBlinkNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().blinkTable(true).period(10));

        tick(0);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 0);

        // Check for blink table property that rows exist for a single tick.
        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        // Ensure multiple rows in one tick.
        tick(100);
        Assert.assertEquals(timeTable.size(), 10);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(10), 100);
    }

    @Test
    public void testBlinkNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().blinkTable(true).period(10));

        tick(15);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);

        tick(21);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(1), 20);
    }

    @Test
    public void testBlinkProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .blinkTable(true)
                .startTime(DateTimeUtils.epochNanosToInstant(10))
                .period(10));

        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        tick(10);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);
    }

    @Test
    public void testBlinkProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .blinkTable(true)
                .startTime(DateTimeUtils.epochNanosToInstant(15))
                .period(10));

        tick(14);
        Assert.assertEquals(timeTable.size(), 0);

        tick(16);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 15);

        tick(34);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(1), 25);
    }

    @Test
    public void testColumnSourceMatch() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        final Long[] longKeys = new Long[] {null, 1000L, 1050L, 1100L, 1025L};
        final Instant[] keys = Arrays.stream(longKeys)
                .map(l -> l == null ? null : DateTimeUtils.epochNanosToInstant(l))
                .toArray(Instant[]::new);
        try (final RowSet match =
                dtColumn.match(false, false, false, null, RowSetFactory.fromRange(100, 110),
                        (Object[]) keys)) {
            Assert.assertEquals(match, RowSetFactory.fromKeys(100, 105, 110));
        }
        try (final RowSet match =
                column.match(false, false, false, null, RowSetFactory.fromRange(100, 110),
                        (Object[]) longKeys)) {
            Assert.assertEquals(match, RowSetFactory.fromKeys(100, 105, 110));
        }
        // inverted
        try (final RowSet match =
                dtColumn.match(true, false, false, null, RowSetFactory.fromRange(100, 110),
                        (Object[]) keys)) {
            Assert.assertEquals(match, RowSetFactory.fromKeys(101, 102, 103, 104, 106, 107, 108, 109));
        }
        try (final RowSet match =
                column.match(true, false, false, null, RowSetFactory.fromRange(100, 110),
                        (Object[]) longKeys)) {
            Assert.assertEquals(match, RowSetFactory.fromKeys(101, 102, 103, 104, 106, 107, 108, 109));
        }
    }

    @Test
    public void testIndexing() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        final DataIndexer dataIndexer = DataIndexer.of(timeTable.getRowSet());

        // Create the index for this table and column.
        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true);
                final RowSet intersectRowSet = RowSetFactory.fromRange(100, 109)) {
            DataIndexer.getOrCreateDataIndex(timeTable, "Timestamp");

            final BasicDataIndex dataIndex =
                    dataIndexer.getDataIndex(dtColumn).transform(
                            DataIndexTransformer.builder()
                                    .intersectRowSet(intersectRowSet)
                                    .build());
            final Table indexTable = dataIndex.table();

            Assert.assertEquals(indexTable.size(), 10);
            try (final CloseableIterator<Instant> keyIt = indexTable.columnIterator(dataIndex.keyColumnNames().get(0));
                    final CloseableIterator<RowSet> rsIt = indexTable.columnIterator(dataIndex.rowSetColumnName())) {
                while (keyIt.hasNext()) {
                    final Instant key = keyIt.next();
                    final RowSet rs = rsIt.next();
                    Assert.assertEquals(rs.size(), 1);
                    Assert.assertEquals(dtColumn.get(rs.firstRowKey()), key);
                }
            }

            final Table riTable = timeTable.updateView(List.of(
                    new ReinterpretedColumn<>("Timestamp", Instant.class, "longTimestamp", long.class)));

            // Create the index for this table and column.
            DataIndexer.getOrCreateDataIndex(riTable, "longTimestamp");

            final BasicDataIndex longDataIndex =
                    Objects.requireNonNull(DataIndexer.getDataIndex(riTable, "longTimestamp")).transform(
                            DataIndexTransformer.builder()
                                    .intersectRowSet(intersectRowSet)
                                    .build());
            final Table longIndexTable = longDataIndex.table();

            Assert.assertEquals(longIndexTable.size(), 10);
            try (final CloseablePrimitiveIteratorOfLong keyIt = new ChunkedLongColumnIterator(
                    longIndexTable.getColumnSource(longDataIndex.keyColumnNames().get(0), long.class),
                    longIndexTable.getRowSet());
                    final CloseableIterator<RowSet> rsIt =
                            longIndexTable.columnIterator(longDataIndex.rowSetColumnName())) {
                while (keyIt.hasNext()) {
                    final long key = keyIt.nextLong();
                    final RowSet rs = rsIt.next();
                    Assert.assertEquals(rs.size(), 1);
                    Assert.assertEquals(column.getLong(rs.firstRowKey()), key);
                }
            }
        }
    }

    @Test
    public void testFillChunkUnordered() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        // noinspection unchecked
        final FillUnordered<Values> fillDtColumn = (FillUnordered<Values>) dtColumn;
        Assert.assertTrue(fillDtColumn.providesFillUnordered());
        // noinspection unchecked
        final FillUnordered<Values> fillLongColumn = (FillUnordered<Values>) column;
        Assert.assertTrue(fillLongColumn.providesFillUnordered());

        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(10)) {
            keys.setSize(0);
            keys.add(109);
            keys.add(100);
            keys.add(103);
            keys.add(102);
            keys.add(109);
            keys.add(106);
            keys.add(100);

            // curr Instant
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<Instant, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), dtColumn.get(keys.get(ii)));
                }
            }

            // prev Instant
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<Instant, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), dtColumn.get(keys.get(ii)));
                }
            }

            // curr long
            try (final ChunkSource.FillContext context = column.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), column.getLong(keys.get(ii)));
                }
            }

            // prev long
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), column.getLong(keys.get(ii)));
                }
            }
        }
    }

    @Test
    public void testConcurrentConstruction() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        build(TimeTable.newBuilder().period(10));
        clock.now = 1;
        try (final SafeCloseable ignored = updateGraph::completeCycleForUnitTests) {
            updateGraph.refreshUpdateSourceForUnitTests(updateSourceCombiner);
            updateGraph.markSourcesRefreshedForUnitTests();
        }
    }
}
