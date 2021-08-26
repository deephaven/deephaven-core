package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.v2.DynamicNode;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.sources.ColumnSource;

import static io.deephaven.db.tables.utils.TableTools.intCol;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;
import static io.deephaven.db.v2.TstUtils.prevTable;
import static io.deephaven.db.v2.sources.chunk.Attributes.Values;

import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;

import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource}.
 */
public class TestReadOnlyRedirectedColumnSource {

    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

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
        final OrderedKeys oks,
        final WritableObjectChunk<String, Values> chunk,
        final long offset) {
        final MutableLong pos = new MutableLong();
        oks.forAllLongs(k -> {
            final String s = (String) cs.get(k);
            assertEquals("offset=" + (pos.intValue() + offset) + ", k=" + k, s,
                chunk.get(pos.intValue()));
            pos.increment();
        });
    }

    private Table doFillAndCheck(
        final Table t,
        final String col,
        final WritableObjectChunk<String, Values> chunk,
        final int sz) {
        final ColumnSource cs = t.getColumnSource(col);
        final Index ix = t.getIndex();
        try (final ColumnSource.FillContext fc = cs.makeFillContext(sz);
            final OrderedKeys.Iterator it = ix.getOrderedKeysIterator()) {
            long offset = 0;
            while (it.hasMore()) {
                final OrderedKeys oks = it.getNextOrderedKeysWithLength(sz);
                cs.fillChunk(fc, chunk, oks);
                doCheck(cs, oks, chunk, offset);
                offset += oks.size();
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
        final WritableObjectChunk<String, Values> chunk =
            WritableObjectChunk.makeWritableChunk(chunkSz);
        while (live.size() < t.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(incFilter::refresh);
            doFillAndCheck(live, "StringsCol", chunk, chunkSz);
        }
    }

    @Test
    public void testMixedFillChunk() {
        final Table a =
            TableTools.emptyTable(1_000_000L).update("A=(long) (Math.random() * 1_000_000L)");

        final Table ab = a.update("B=A % 2");
        final Table expected = ab.by("B", "A").sort("A");

        final Table redirected = a.sort("A").update("B=A % 2");
        final Table actual = redirected.by("B", "A");

        assertTableEquals(expected, actual);
    }

    @Test
    public void testIds6196() {
        final Boolean[] ids6196_values = new Boolean[] {true, null, false};
        QueryScope.addParam("ids6196_values", ids6196_values);

        final QueryTable qt = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(6),
            intCol("IntVal", 0, 1, 2, 3, 4, 5));

        final Table a = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> qt
            .update("I2=3+IntVal", "BoolVal=ids6196_values[IntVal % ids6196_values.length]"));
        TableTools.showWithIndex(a);
        final Table b = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> a.naturalJoin(a, "I2=IntVal", "BoolVal2=BoolVal"));
        TableTools.showWithIndex(b);

        final TByteList byteList = new TByteArrayList(6);
        final ColumnSource reinterpretedB = b.getColumnSource("BoolVal2").reinterpret(byte.class);
        b.getIndex().forAllLongs(x -> {
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
            final ByteChunk<? extends Values> result =
                reinterpretedB.getChunk(context, b.getIndex()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(expecteds, chunkResult);
        }

        final Table c = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> a.naturalJoin(b, "I2=IntVal", "BoolVal3=BoolVal2"));
        TableTools.showWithIndex(c);
        final ColumnSource reinterpretedC = c.getColumnSource("BoolVal3").reinterpret(byte.class);
        byteList.clear();
        b.getIndex().forAllLongs(x -> {
            final byte value = reinterpretedC.getByte(x);
            System.out.println(value);
            byteList.add(value);
        });

        byteList.clear();
        b.getIndex().forAllLongs(x -> {
            final byte value = reinterpretedC.getPrevByte(x);
            System.out.println(value);
            byteList.add(value);
        });

        final byte[] nullBytes = new byte[6];
        Arrays.fill(nullBytes, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
        assertArrayEquals(nullBytes, byteList.toArray());

        try (final ChunkSource.GetContext context = reinterpretedC.makeGetContext(6)) {
            final ByteChunk<? extends Values> result =
                reinterpretedC.getChunk(context, b.getIndex()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(nullBytes, chunkResult);
        }

        try (final ChunkSource.GetContext context = reinterpretedC.makeGetContext(6)) {
            final ByteChunk<? extends Values> result =
                reinterpretedC.getPrevChunk(context, b.getIndex()).asByteChunk();
            final byte[] chunkResult = new byte[6];
            result.copyToTypedArray(0, chunkResult, 0, 6);
            assertArrayEquals(nullBytes, chunkResult);
        }

        final Table captured = LiveTableMonitor.DEFAULT.sharedLock()
            .computeLocked(() -> TableTools.emptyTable(1).snapshot(c));
        TableTools.showWithIndex(captured);

        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        TstUtils.addToTable(qt, Index.FACTORY.getFlatIndex(3), intCol("IntVal", 1, 2, 3));
        qt.notifyListeners(Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex(),
            Index.FACTORY.getFlatIndex(3));

        LiveTableMonitor.DEFAULT.flushAllNormalNotificationsForUnitTests();

        System.out.println("A:");
        TableTools.showWithIndex(a);

        // checks chunked
        System.out.println("C:");
        TableTools.showWithIndex(c);
        System.out.println("prev(C):");
        TableTools.showWithIndex(prevTable(c));

        assertTableEquals(captured, prevTable(c));

        // checks unchunked
        byteList.clear();
        b.getIndex().forAllLongs(x -> {
            final byte value = reinterpretedB.getPrevByte(x);
            System.out.println(value);
            byteList.add(value);
        });
        assertArrayEquals(expecteds, byteList.toArray());

        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
    }
}
