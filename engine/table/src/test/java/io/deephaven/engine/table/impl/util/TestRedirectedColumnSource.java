package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.InMemoryTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.table.ColumnSource;

import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.engine.table.impl.TstUtils.prevTable;

import io.deephaven.chunk.attributes.Values;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;

import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import io.deephaven.test.junit4.EngineCleanup;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static io.deephaven.engine.util.TableTools.showWithRowSet;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link RedirectedColumnSource}.
 */
public class TestRedirectedColumnSource {

    @Rule
    public final EngineCleanup base = new EngineCleanup();


    private Table makeTable() {
        final int sz = 100 * 1000;
        final String[] strs = new String[sz];
        final int[] is = new int[sz];
        for (int i = 0; i < sz; ++i) {
            final int v = sz - 1 - i;
            strs[i] = "k" + v;
            is[i] = v;
        }
        final Table t = new InMemoryTable(
                new String[] {"StringsCol", "IntsCol"},
                new Object[] {strs, is});
        ((DynamicNode) t).setRefreshing(true);
        return t;
    }

    private void doCheck(
            final ColumnSource cs,
            final RowSequence rs,
            final WritableObjectChunk<String, Values> chunk,
            final long offset) {
        final MutableLong pos = new MutableLong();
        rs.forAllRowKeys(k -> {
            final String s = (String) cs.get(k);
            assertEquals("offset=" + (pos.intValue() + offset) + ", k=" + k, s, chunk.get(pos.intValue()));
            pos.increment();
        });
    }

    private Table doFillAndCheck(
            final Table t,
            final String col,
            final WritableObjectChunk<String, Values> chunk,
            final int sz) {
        final ColumnSource cs = t.getColumnSource(col);
        final RowSet ix = t.getRowSet();
        try (final ColumnSource.FillContext fc = cs.makeFillContext(sz);
                final RowSequence.Iterator it = ix.getRowSequenceIterator()) {
            long offset = 0;
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(sz);
                cs.fillChunk(fc, chunk, rs);
                doCheck(cs, rs, chunk, offset);
                offset += rs.size();
            }
        }
        return t;
    }

    @Test
    public void testFillChunk() {
        final Table t = makeTable();
        final int steps = 10;
        final int stepSz = (int) (t.size() / steps);
        final IncrementalReleaseFilter incFilter = new IncrementalReleaseFilter(stepSz, stepSz);
        final Table live = t.where(incFilter).sort("IntsCol");
        final int chunkSz = stepSz - 7;
        try (final WritableObjectChunk<String, Values> chunk = WritableObjectChunk.makeWritableChunk(chunkSz)) {
            while (live.size() < t.size()) {
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(incFilter::run);
                doFillAndCheck(live, "StringsCol", chunk, chunkSz);
            }
        }
    }

    @Test
    public void testMixedFillChunk() {
        final Table a = TableTools.emptyTable(1_000_000L).update("A=(long) (Math.random() * 1_000_000L)");

        final Table ab = a.update("B=A % 2");
        final Table expected = ab.selectDistinct("B", "A").sort("A");

        final Table redirected = a.sort("A").update("B=A % 2");
        final Table actual = redirected.selectDistinct("B", "A");

        assertTableEquals(expected, actual);
    }

    @Test
    public void testIds6196() {
        final Boolean[] ids6196_values = new Boolean[] {true, null, false};
        QueryScope.addParam("ids6196_values", ids6196_values);

        final QueryTable qt =
                TstUtils.testRefreshingTable(RowSetFactory.flat(6).toTracking(),
                        intCol("IntVal", 0, 1, 2, 3, 4, 5));

        final Table a = UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(
                () -> qt.update("I2=3+IntVal", "BoolVal=ids6196_values[IntVal % ids6196_values.length]"));
        showWithRowSet(a);
        final Table b = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> a.naturalJoin(a, "I2=IntVal", "BoolVal2=BoolVal"));
        showWithRowSet(b);

        final TByteList byteList = new TByteArrayList(6);
        final ColumnSource reinterpretedB = b.getColumnSource("BoolVal2").reinterpret(byte.class);
        b.getRowSet().forAllRowKeys(x -> {
            final byte value = reinterpretedB.getByte(x);
            System.out.println(value);
            byteList.add(value);
        });
        final byte[] expecteds = new byte[6];
        final MutableInt idx = new MutableInt();
        Stream.of(true, null, false, null, null, null).forEach(boolVal -> {
            expecteds[idx.intValue()] = BooleanUtils.booleanAsByte(boolVal);
            idx.increment();
        });
        assertArrayEquals(expecteds, byteList.toArray());

        try (final ChunkSource.GetContext context = reinterpretedB.makeGetContext(6)) {
            final ByteChunk<? extends Values> result = reinterpretedB.getChunk(context, b.getRowSet()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(expecteds, chunkResult);
        }

        final Table c = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> a.naturalJoin(b, "I2=IntVal", "BoolVal3=BoolVal2"));
        showWithRowSet(c);
        final ColumnSource reinterpretedC = c.getColumnSource("BoolVal3").reinterpret(byte.class);
        byteList.clear();
        b.getRowSet().forAllRowKeys(x -> {
            final byte value = reinterpretedC.getByte(x);
            System.out.println(value);
            byteList.add(value);
        });

        byteList.clear();
        b.getRowSet().forAllRowKeys(x -> {
            final byte value = reinterpretedC.getPrevByte(x);
            System.out.println(value);
            byteList.add(value);
        });

        final byte[] nullBytes = new byte[6];
        Arrays.fill(nullBytes, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
        assertArrayEquals(nullBytes, byteList.toArray());

        try (final ChunkSource.GetContext context = reinterpretedC.makeGetContext(6)) {
            final ByteChunk<? extends Values> result = reinterpretedC.getChunk(context, b.getRowSet()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(nullBytes, chunkResult);
        }

        try (final ChunkSource.GetContext context = reinterpretedC.makeGetContext(6)) {
            final ByteChunk<? extends Values> result =
                    reinterpretedC.getPrevChunk(context, b.getRowSet()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(nullBytes, chunkResult);
        }

        final Table captured =
                UpdateGraphProcessor.DEFAULT.sharedLock().computeLocked(() -> TableTools.emptyTable(1).snapshot(c));
        showWithRowSet(captured);

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        TstUtils.addToTable(qt, RowSetFactory.flat(3), intCol("IntVal", 1, 2, 3));
        qt.notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), RowSetFactory.flat(3));

        UpdateGraphProcessor.DEFAULT.flushAllNormalNotificationsForUnitTests();

        System.out.println("A:");
        showWithRowSet(a);

        // checks chunked
        System.out.println("C:");
        showWithRowSet(c);
        System.out.println("prev(C):");
        showWithRowSet(prevTable(c));

        assertTableEquals(captured, prevTable(c));

        // checks unchunked
        byteList.clear();
        b.getRowSet().forAllRowKeys(x -> {
            final byte value = reinterpretedB.getPrevByte(x);
            System.out.println(value);
            byteList.add(value);
        });
        assertArrayEquals(expecteds, byteList.toArray());

        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }
}
