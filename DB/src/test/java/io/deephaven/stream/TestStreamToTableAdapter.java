package io.deephaven.stream;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.sources.chunk.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

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

        final StreamPublisher streamPublisher = new StreamPublisher() {
            @Override
            public void register(@NotNull StreamConsumer consumer) {

            }

            @Override
            public void flush() {

            }
        };



        final StreamToTableAdapter adapter = new StreamToTableAdapter(tableDefinition, streamPublisher, LiveTableMonitor.DEFAULT);
        final Table result = adapter.table();
        TstUtils.assertTableEquals(empty, result);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);
        TstUtils.assertTableEquals(empty, result);

        final WritableChunk [] chunks = new WritableChunk[4];
        final WritableObjectChunk<Object, Attributes.Values> woc = WritableObjectChunk.makeWritableChunk(2);
        chunks[0] = woc;
        woc.set(0, "Bill");
        woc.set(1, "Ted");
        final WritableIntChunk<Attributes.Values> wic = WritableIntChunk.makeWritableChunk(2);
        chunks[1] = wic;
        wic.set(0, 2);
        wic.set(1, 3);
        final WritableLongChunk<Attributes.Values> wlc = WritableLongChunk.makeWritableChunk(2);
        chunks[2] = wlc;
        wlc.set(0, 4);
        wlc.set(1, 5);
        final WritableDoubleChunk<Attributes.Values> wdc = WritableDoubleChunk.makeWritableChunk(2);
        chunks[3] = wdc;
        wdc.set(0, Math.PI);
        wdc.set(1, Math.E);

        adapter.accept();

        TstUtils.assertTableEquals(empty, result);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(adapter::refresh);

        final Table expect1 = TableTools.newTable(col("S", "Bill", "Ted"), intCol("I", 2, 3), longCol("L", 4L, 5L), doubleCol("D", Math.PI, Math.E));
        TstUtils.assertTableEquals(expect1, result);
    }
}
