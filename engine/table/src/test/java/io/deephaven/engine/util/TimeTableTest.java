package io.deephaven.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeProvider;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class TimeTableTest extends RefreshingTableTestCase {

    private MutableLong now;
    private TimeProvider timeProvider;
    private UpdateSourceCombiner updateSourceCombiner;
    private QueryTable timeTable;
    private TableUpdateValidator validator;
    private ColumnSource<Long> column;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        now = new MutableLong(0);
        timeProvider = () -> new DateTime(now.longValue());
        updateSourceCombiner = new UpdateSourceCombiner();
    }

    @Override
    protected void tearDown() throws Exception {
        validator.deepValidation();

        now = null;
        timeProvider = null;
        updateSourceCombiner = null;
        timeTable = null;
        validator = null;
        column = null;

        super.tearDown();
    }

    private void build(TimeTable.Builder builder) {
        timeTable = builder
                .registrar(updateSourceCombiner)
                .timeProvider(timeProvider)
                .build();
        column = timeTable.getColumnSource("Timestamp").reinterpret(long.class);
        validator = TableUpdateValidator.make(timeTable);
    }

    private void tick(long tm) {
        now.setValue(tm);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(updateSourceCombiner::run);
        validator.validate();
    }

    @Test
    public void testNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().period(10));

        tick(0);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 0);

        tick(9);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);

        tick(10);
        Assert.eq(timeTable.size(), "timeTable.size()", 2);
        Assert.eq(column.getLong(1), "column.getLong(1)", 10);

        // Check that it will tick multiple rows.
        tick(100);
        Assert.eq(timeTable.size(), "timeTable.size()", 11);
    }

    @Test
    public void testNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().period(10));

        tick(15);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 10);

        tick(24);
        Assert.eq(timeTable.size(), "timeTable.size()", 2);
        Assert.eq(column.getLong(1), "column.getLong(1)", 20);
    }

    @Test
    public void testProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.nanosToTime(10))
                .period(10));

        tick(9);
        Assert.eq(timeTable.size(), "timeTable.size()", 0);

        tick(10);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 10);
    }

    @Test
    public void testProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.nanosToTime(15))
                .period(10));

        tick(14);
        Assert.eq(timeTable.size(), "timeTable.size()", 0);

        tick(16);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 15);

        tick(34);
        Assert.eq(timeTable.size(), "timeTable.size()", 2);
        Assert.eq(column.getLong(1), "column.getLong(1)", 25);
    }

    @Test
    public void testStreamNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().streamTable(true).period(10));

        tick(0);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 0);

        // Check for stream table property that rows exist for a single tick.
        tick(9);
        Assert.eq(timeTable.size(), "timeTable.size()", 0);

        // Ensure multiple rows in one tick.
        tick(100);
        Assert.eq(timeTable.size(), "timeTable.size()", 10);
        Assert.eq(timeTable.getRowSet().firstRowKey(), "timeTable.getRowSet().firstRowKey()", 1);
        Assert.eq(column.getLong(10), "column.getLong(10)", 100);
    }

    @Test
    public void testStreamNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().streamTable(true).period(10));

        tick(15);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 10);

        tick(21);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(timeTable.getRowSet().firstRowKey(), "timeTable.getRowSet().firstRowKey()", 1);
        Assert.eq(column.getLong(1), "column.getLong(1)", 20);
    }

    @Test
    public void testStreamProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .streamTable(true)
                .startTime(DateTimeUtils.nanosToTime(10))
                .period(10));

        tick(9);
        Assert.eq(timeTable.size(), "timeTable.size()", 0);

        tick(10);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 10);
    }

    @Test
    public void testStreamProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .streamTable(true)
                .startTime(DateTimeUtils.nanosToTime(15))
                .period(10));

        tick(14);
        Assert.eq(timeTable.size(), "timeTable.size()", 0);

        tick(16);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(column.getLong(0), "column.getLong(0)", 15);

        tick(34);
        Assert.eq(timeTable.size(), "timeTable.size()", 1);
        Assert.eq(timeTable.getRowSet().firstRowKey(), "timeTable.getRowSet().firstRowKey()", 1);
        Assert.eq(column.getLong(1), "column.getLong(0)", 25);
    }

    @Test
    public void testColumnSourceMatch() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<DateTime> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.eq(timeTable.size(), "timeTable.size()", 201);

        final Long[] longKeys = new Long[] {null, 1000L, 1050L, 1100L};
        final DateTime[] keys = Arrays.stream(longKeys)
                .map(l -> l == null ? null : DateTimeUtils.nanosToTime(l))
                .toArray(DateTime[]::new);
        try (final RowSet match =
                dtColumn.match(false, false, false, RowSetFactory.fromRange(100, 110), (Object[]) keys)) {
            Assert.equals(match, "match", RowSetFactory.fromKeys(100, 105, 110));
        }
        try (final RowSet match =
                column.match(false, false, false, RowSetFactory.fromRange(100, 110), (Object[]) longKeys)) {
            Assert.equals(match, "match", RowSetFactory.fromKeys(100, 105, 110));
        }
        // inverted
        try (final RowSet match =
                dtColumn.match(true, false, false, RowSetFactory.fromRange(100, 110), (Object[]) keys)) {
            Assert.equals(match, "match", RowSetFactory.fromKeys(101, 102, 103, 104, 106, 107, 108, 109));
        }
        try (final RowSet match =
                column.match(true, false, false, RowSetFactory.fromRange(100, 110), (Object[]) longKeys)) {
            Assert.equals(match, "match", RowSetFactory.fromKeys(101, 102, 103, 104, 106, 107, 108, 109));
        }
    }

    @Test
    public void testGetValuesMapping() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<DateTime> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.eq(timeTable.size(), "timeTable.size()", 201);

        final Map<DateTime, RowSet> dtMap = dtColumn.getValuesMapping(RowSetFactory.fromRange(100, 109));
        Assert.eq(dtMap.size(), "dtMap.size()", 10);
        dtMap.forEach((tm, rows) -> {
            Assert.eq(rows.size(), "rows.size()", 1);
            Assert.equals(dtColumn.get(rows.firstRowKey()), "dtColumn.get(rows.firstRowKey())", tm, "tm");
        });

        Map<Long, RowSet> longMap = column.getValuesMapping(RowSetFactory.fromRange(100, 109));
        Assert.eq(longMap.size(), "dtMap.size()", 10);
        longMap.forEach((tm, rows) -> {
            Assert.eq(rows.size(), "rows.size()", 1);
            Assert.equals(column.get(rows.firstRowKey()), "dtColumn.get(rows.firstRowKey())", tm, "tm");
        });
    }

    @Test
    public void testFillChunkUnordered() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<DateTime> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.eq(timeTable.size(), "timeTable.size()", 201);

        final FillUnordered fillDtColumn = (FillUnordered) dtColumn;
        Assert.eqTrue(fillDtColumn.providesFillUnordered(), "fillDtColumn.providesFillUnordered()");
        final FillUnordered fillLongColumn = (FillUnordered) column;
        Assert.eqTrue(fillLongColumn.providesFillUnordered(), "fillLongColumn.providesFillUnordered()");

        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(10)) {
            keys.setSize(0);
            keys.add(109);
            keys.add(100);
            keys.add(103);
            keys.add(102);
            keys.add(109);
            keys.add(106);
            keys.add(100);

            // curr DateTime
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<DateTime, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillChunkUnordered(context, dest, keys);
                Assert.eq(dest.size(), "dest.size()", keys.size(), "keys.size()");
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.equals(dest.get(ii), "dest.get(ii)", dtColumn.get(keys.get(ii)));
                }
            }

            // prev DateTime
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<DateTime, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.eq(dest.size(), "dest.size()", keys.size(), "keys.size()");
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.equals(dest.get(ii), "dest.get(ii)", dtColumn.get(keys.get(ii)));
                }
            }

            // curr long
            try (final ChunkSource.FillContext context = column.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillChunkUnordered(context, dest, keys);
                Assert.eq(dest.size(), "dest.size()", keys.size(), "keys.size()");
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.equals(dest.get(ii), "dest.get(ii)", column.getLong(keys.get(ii)));
                }
            }

            // prev long
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.eq(dest.size(), "dest.size()", keys.size(), "keys.size()");
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.equals(dest.get(ii), "dest.get(ii)", column.get(keys.get(ii)));
                }
            }
        }
    }
}
