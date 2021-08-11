package io.deephaven.stream;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.SimpleShiftAwareListener;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.util.BooleanUtils;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.db.tables.utils.TableTools.*;

public class TestStreamToTableAdapter {
    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testSimple() {
        final TableDefinition tableDefinition = new TableDefinition(Arrays.asList(String.class, int.class, long.class, double.class), Arrays.asList("S", "I", "L", "D"));
        final DynamicTable empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToTableAdapter adapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        final DynamicTable result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleShiftAwareListener listener = new SimpleShiftAwareListener(result);
        result.listenForUpdates(listener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Attributes.Values> [] chunks = new WritableChunk[4];
        WritableObjectChunk<Object, Attributes.Values> woc;
        WritableIntChunk<Attributes.Values> wic;
        WritableLongChunk<Attributes.Values> wlc;
        WritableDoubleChunk<Attributes.Values> wdc;

        chunks[0] = woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic = WritableIntChunk.makeWritableChunk(2);
        chunks[2] = wlc = WritableLongChunk.makeWritableChunk(2);
        chunks[3] = wdc = WritableDoubleChunk.makeWritableChunk(2);

        woc.set(0, "Bill");
        woc.set(1, "Ted");
        wic.set(0, 2);
        wic.set(1, 3);
        wlc.set(0, 4);
        wlc.set(1, 5);
        wdc.set(0, Math.PI);
        wdc.set(1, Math.E);

        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect1 = TableTools.newTable(col("S", "Bill", "Ted"), intCol("I", 2, 3), longCol("L", 4L, 5L), doubleCol("D", Math.PI, Math.E));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());

        chunks[0] = woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic = WritableIntChunk.makeWritableChunk(2);
        chunks[2] = wlc = WritableLongChunk.makeWritableChunk(2);
        chunks[3] = wdc = WritableDoubleChunk.makeWritableChunk(2);
        woc.set(0, "Ren");
        woc.set(1, "Stimpy");

        wic.set(0, 7);
        wic.set(1, 8);

        wlc.set(0, 9);
        wlc.set(1, 10);

        wdc.set(0, 11.1);
        wdc.set(1, 12.2);

        adapter.accept(chunks);

        chunks[0] = woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic = WritableIntChunk.makeWritableChunk(2);
        chunks[2] = wlc = WritableLongChunk.makeWritableChunk(2);
        chunks[3] = wdc = WritableDoubleChunk.makeWritableChunk(2);
        woc.set(0, "Jekyll");
        woc.set(1, "Hyde");

        wic.set(0, 13);
        wic.set(1, 14);

        wlc.set(0, 15);
        wlc.set(1, 16);

        wdc.set(0, 17.7);
        wdc.set(1, 18.8);
        adapter.accept(chunks);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(4), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect2 = TableTools.newTable(col("S", "Ren", "Stimpy", "Jekyll", "Hyde"), intCol("I", 7, 8, 13, 14), longCol("L", 9, 10, 15, 16), doubleCol("D", 11.1, 12.2, 17.7, 18.8));
        TstUtils.assertTableEquals(expect2, result);

        chunks[0] = woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic = WritableIntChunk.makeWritableChunk(2);
        chunks[2] = wlc = WritableLongChunk.makeWritableChunk(2);
        chunks[3] = wdc = WritableDoubleChunk.makeWritableChunk(2);
        woc.set(0, "Ben");
        woc.set(1, "Jerry");

        wic.set(0, 19);
        wic.set(1, 20);

        wlc.set(0, 21);
        wlc.set(1, 22);

        wdc.set(0, 23.3);
        wdc.set(1, 24.4);

        adapter.accept(chunks);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(4), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect3 = TableTools.newTable(col("S", "Ben", "Jerry"), intCol("I", 19, 20), longCol("L", 21, 22), doubleCol("D", 23.3, 24.4));
        TstUtils.assertTableEquals(expect3, result);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());
        TstUtils.assertTableEquals(empty, result);
    }

    @Test
    public void testWrappedTypes() {
        final TableDefinition tableDefinition = new TableDefinition(Arrays.asList(String.class, Boolean.class, DBDateTime.class), Arrays.asList("S", "B", "D"));
        final DynamicTable empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToTableAdapter adapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        final DynamicTable result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleShiftAwareListener listener = new SimpleShiftAwareListener(result);
        result.listenForUpdates(listener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Attributes.Values> [] chunks = new WritableChunk[3];
        final WritableObjectChunk<Object, Attributes.Values> woc = WritableObjectChunk.makeWritableChunk(3);
        chunks[0] = woc;
        woc.set(0, "Collins");
        woc.set(1, "Armstrong");
        woc.set(2, "Aldrin");
        final WritableByteChunk<Attributes.Values> wic = WritableByteChunk.makeWritableChunk(3);
        chunks[1] = wic;
        wic.set(0, BooleanUtils.booleanAsByte(true));
        wic.set(1, BooleanUtils.booleanAsByte(false));
        wic.set(2, BooleanUtils.booleanAsByte(null));
        final WritableLongChunk<Attributes.Values> wlc = WritableLongChunk.makeWritableChunk(3);
        chunks[2] = wlc;
        final DBDateTime dt1 = DBTimeUtils.convertDateTime("2021-04-28T12:00:00 NY");
        wlc.set(0, dt1.getNanos());
        final DBDateTime dt2 = DBTimeUtils.convertDateTime("2012-08-25T12:00:00 NY");
        wlc.set(1, dt2.getNanos());
        final DBDateTime dt3 = DBTimeUtils.convertDateTime("2030-01-20T12:00:00 NY");
        wlc.set(2, dt3.getNanos());

        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(3), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect1 = TableTools.newTable(col("S", "Collins", "Armstrong", "Aldrin"), col("B", true, false, null), col("D", dt1, dt2, dt3));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(3), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);
    }

    @Test
    public void testArrayTypes() {
        final TableDefinition tableDefinition = new TableDefinition(Arrays.asList(String[].class, int[].class), Arrays.asList("SA", "IA"));
        final DynamicTable empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToTableAdapter adapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        final DynamicTable result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleShiftAwareListener listener = new SimpleShiftAwareListener(result);
        result.listenForUpdates(listener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Attributes.Values> [] chunks = new WritableChunk[2];
        final WritableObjectChunk<String[], Attributes.Values> woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[0] = woc;
        woc.set(0, new String[]{"Gagarin", "Tereshkova"});
        woc.set(1, new String[]{});
        final WritableObjectChunk<int[], Attributes.Values> wic = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic;
        wic.set(0, new int[]{1, 2, 3});
        wic.set(1, new int[]{4, 5, 6});

        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect1 = TableTools.newTable(col("SA", new String[]{"Gagarin", "Tereshkova"}, CollectionUtil.ZERO_LENGTH_STRING_ARRAY), col("IA", new int[]{1, 2, 3}, new int[]{4, 5, 6}));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(2), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);
    }

    @Test
    public void testBig() {
        final TableDefinition tableDefinition = new TableDefinition(Collections.singletonList(long.class), Arrays.asList("L"));
        final DynamicTable empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToTableAdapter adapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        final DynamicTable result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleShiftAwareListener listener = new SimpleShiftAwareListener(result);
        result.listenForUpdates(listener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final long [] exVals = new long[4048];
        int pos = 0;

        final WritableChunk<Attributes.Values> [] chunks = new WritableChunk[1];
        WritableLongChunk<Attributes.Values> wlc;
        chunks[0] = wlc = WritableLongChunk.makeWritableChunk(2048);
        wlc.setSize(2048);
        for (int ii = 0; ii < wlc.size(); ++ii) {
            exVals[pos++] = ii;
            wlc.set(ii, ii);
        }
        adapter.accept(chunks);

        chunks[0] = wlc = WritableLongChunk.makeWritableChunk(2000);
        wlc.setSize(2000);
        for (int ii = 0; ii < wlc.size(); ++ii) {
            wlc.set(ii, 10000 + ii);
            exVals[pos++] = 10000 + ii;
        }
        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(4048), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        final Table expect1 = TableTools.newTable(longCol("L", exVals));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(Index.FACTORY.getFlatIndex(4048), listener.getUpdate().removed);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().added);
        TestCase.assertEquals(Index.FACTORY.getEmptyIndex(), listener.getUpdate().modified);
        TestCase.assertEquals(IndexShiftData.EMPTY, listener.getUpdate().shifted);
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet);

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TestCase.assertEquals(0, listener.getCount());
    }


    private static class DummyStreamPublisher implements StreamPublisher {
        @Override
        public void register(@NotNull StreamConsumer consumer) {

        }

        @Override
        public void flush() {

        }
    }
}
