//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.SimpleListener;
import io.deephaven.chunk.*;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.ArrayTypeUtils;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static io.deephaven.engine.util.TableTools.*;

public class TestStreamToBlinkTableAdapter {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testSimple() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("S", "I", "L", "D"),
                List.of(String.class, int.class, long.class, double.class));
        final Table empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                tableDefinition, streamPublisher, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Values>[] chunks = new WritableChunk[4];
        WritableObjectChunk<Object, Values> woc;
        WritableIntChunk<Values> wic;
        WritableLongChunk<Values> wlc;
        WritableDoubleChunk<Values> wdc;

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

        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect1 = TableTools.newTable(col("S", "Bill", "Ted"), intCol("I", 2, 3), longCol("L", 4L, 5L),
                doubleCol("D", Math.PI, Math.E));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
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
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(4), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect2 = TableTools.newTable(col("S", "Ren", "Stimpy", "Jekyll", "Hyde"),
                intCol("I", 7, 8, 13, 14), longCol("L", 9, 10, 15, 16), doubleCol("D", 11.1, 12.2, 17.7, 18.8));
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
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.flat(4), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect3 = TableTools.newTable(col("S", "Ben", "Jerry"), intCol("I", 19, 20), longCol("L", 21, 22),
                doubleCol("D", 23.3, 24.4));
        TstUtils.assertTableEquals(expect3, result);

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(0, listener.getCount());
        TstUtils.assertTableEquals(empty, result);
    }

    @Test
    public void testWrappedTypes() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("S", "B", "T"),
                List.of(String.class, Boolean.class, Instant.class));
        final Table empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, streamPublisher,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Values>[] chunks = new WritableChunk[3];
        final WritableObjectChunk<Object, Values> woc = WritableObjectChunk.makeWritableChunk(3);
        chunks[0] = woc;
        woc.set(0, "Collins");
        woc.set(1, "Armstrong");
        woc.set(2, "Aldrin");
        final WritableByteChunk<Values> wic = WritableByteChunk.makeWritableChunk(3);
        chunks[1] = wic;
        wic.set(0, BooleanUtils.booleanAsByte(true));
        wic.set(1, BooleanUtils.booleanAsByte(false));
        wic.set(2, BooleanUtils.booleanAsByte(null));
        final WritableLongChunk<Values> wlc = WritableLongChunk.makeWritableChunk(3);
        chunks[2] = wlc;
        final Instant instant1 = DateTimeUtils.parseInstant("2021-04-28T12:00:00 NY");
        wlc.set(0, DateTimeUtils.epochNanos(instant1));
        final Instant instant2 = DateTimeUtils.parseInstant("2012-08-25T12:00:00 NY");
        wlc.set(1, DateTimeUtils.epochNanos(instant2));
        final Instant instant3 = DateTimeUtils.parseInstant("2030-01-20T12:00:00 NY");
        wlc.set(2, DateTimeUtils.epochNanos(instant3));

        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(3), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect1 = TableTools.newTable(col("S", "Collins", "Armstrong", "Aldrin"),
                col("B", true, false, null), col("T", instant1, instant2, instant3));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(3), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());
    }

    @Test
    public void testArrayTypes() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("SA", "IA"),
                List.of(String[].class, int[].class));
        final Table empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                tableDefinition, streamPublisher, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final WritableChunk<Values>[] chunks = new WritableChunk[2];
        final WritableObjectChunk<String[], Values> woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[0] = woc;
        woc.set(0, new String[] {"Gagarin", "Tereshkova"});
        woc.set(1, new String[] {});
        final WritableObjectChunk<int[], Values> wic = WritableObjectChunk.makeWritableChunk(2);
        chunks[1] = wic;
        wic.set(0, new int[] {1, 2, 3});
        wic.set(1, new int[] {4, 5, 6});

        adapter.accept(chunks);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect1 = TableTools.newTable(
                col("SA", new String[] {"Gagarin", "Tereshkova"}, ArrayTypeUtils.EMPTY_STRING_ARRAY),
                col("IA", new int[] {1, 2, 3}, new int[] {4, 5, 6}));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(2), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());
    }

    @Test
    public void testBig() {
        final TableDefinition tableDefinition = TableDefinition.from(List.of("L"), List.of(long.class));
        final Table empty = TableTools.newTable(tableDefinition);

        final StreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                tableDefinition, streamPublisher, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(0, listener.getCount());

        final long[] exVals = new long[4048];
        int pos = 0;

        final WritableChunk<Values>[] chunks = new WritableChunk[1];
        WritableLongChunk<Values> wlc;
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

        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(4048), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        final Table expect1 = TableTools.newTable(longCol("L", exVals));
        TstUtils.assertTableEquals(expect1, result);

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);

        TstUtils.assertTableEquals(empty, result);
        TestCase.assertEquals(1, listener.getCount());
        TestCase.assertEquals(RowSetFactory.flat(4048), listener.getUpdate().removed());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().added());
        TestCase.assertEquals(RowSetFactory.empty(), listener.getUpdate().modified());
        TestCase.assertEquals(RowSetShiftData.EMPTY, listener.getUpdate().shifted());
        TestCase.assertEquals(ModifiedColumnSet.EMPTY, listener.getUpdate().modifiedColumnSet());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(0, listener.getCount());

        listener.reset();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertEquals(0, listener.getCount());
    }

    @Test
    public void testError() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("S", "I", "L", "D"),
                List.of(String.class, int.class, long.class, double.class));
        final DummyStreamPublisher streamPublisher = new DummyStreamPublisher();

        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                tableDefinition, streamPublisher, ExecutionContext.getContext().getUpdateGraph(), "test");
        final Table result = adapter.table();

        final MutableBoolean listenerFailed = new MutableBoolean();
        final SimpleListener listener = new SimpleListener(result) {
            @Override
            public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                listenerFailed.setTrue();
            }
        };
        result.addUpdateListener(listener);

        streamPublisher.fail = true;
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(adapter::run);
        TestCase.assertTrue(listenerFailed.booleanValue());
    }

    @Test
    public void testCleanup() {
        final TableDefinition tableDefinition = TableDefinition.from(
                List.of("O", "B", "S", "I", "L", "F", "D", "C"),
                List.of(String.class, byte.class, short.class, int.class, long.class, float.class, double.class,
                        char.class));
        final Table tableToAdd = emptyTable(ChunkPoolConstants.SMALLEST_POOLED_CHUNK_CAPACITY).updateView(
                "O=Long.toString(ii)", "B=(byte)ii", "S=(short)ii", "I=(int)ii", "L=ii", "F=(float)ii",
                "D=(double)ii", "C=(char)ii");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final TablePublisher tablePublisher = TablePublisher.of("Test", tableDefinition, null, null);
            // Add buffered chunks
            tablePublisher.add(tableToAdd);
            // Move buffered chunks to current
            updateGraph.runWithinUnitTestCycle(() -> {
            });
            // Add more buffered chunks
            tablePublisher.add(tableToAdd);
            // Move current to previous, buffered to current
            updateGraph.runWithinUnitTestCycle(() -> {
            });
            // Add even more buffered chunks
            tablePublisher.add(tableToAdd);
        }
        ChunkPoolReleaseTracking.check();
    }

    private static class DummyStreamPublisher implements StreamPublisher {

        private boolean fail;

        @Override
        public void register(@NotNull StreamConsumer consumer) {}

        @Override
        public void flush() {
            if (fail) {
                fail = false;
                throw new RuntimeException("I am a fake failure");
            }
        }

        @Override
        public void shutdown() {}
    }
}
