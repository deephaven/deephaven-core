//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.ref.Reference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A filter that respects a barrier is pushed down only over the keys of rows its declarer kept, at every tier that runs
 * a filter over a structure wider than its selection.
 */
public class BarrierActivePushdownTest {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final int ROWS = 200_000;
    private static final int KEYS = 100;
    /**
     * The declarer keeps the keys whose id is below this: half the keys, and every row of them, so that a single row
     * group's selection spans more than one window of the bounded dictionary scan.
     */
    private static final int VISIBLE_BELOW = KEYS / 2;

    private static final Set<String> SEEN = ConcurrentHashMap.newKeySet();
    private static final AtomicLong CALLS = new AtomicLong();

    /** The respecter's user function: records every key it is shown, and counts its calls. */
    public static boolean see(final String key) {
        if (key == null) {
            return false;
        }
        CALLS.incrementAndGet();
        SEEN.add(key);
        return Math.floorMod(key.hashCode(), 2) == 0;
    }

    @Before
    public void setUp() {
        ExecutionContext.getContext().getQueryLibrary().importClass(BarrierActivePushdownTest.class);
    }

    /**
     * Scatters the keys, so that row groups differ at the same offsets and reading the wrong rows changes the result.
     */
    public static int keyId(final long ii) {
        return (int) (((ii * 0x9E3779B97F4A7C15L) >>> 33) % KEYS);
    }

    private static Table data() {
        return TableTools.emptyTable(ROWS).select(
                "Row = ii", "KeyId = BarrierActivePushdownTest.keyId(ii)", "Key = `K` + KeyId");
    }

    private static Set<String> visibleKeys() {
        return java.util.stream.IntStream.range(0, VISIBLE_BELOW).mapToObj(id -> "K" + id).collect(Collectors.toSet());
    }

    /**
     * Applies the declarer and then the respecter, with a barrier between them or, as the unbounded control, none.
     */
    private static Table filter(final Table source, final boolean withBarrier) {
        SEEN.clear();
        CALLS.set(0);
        final WhereFilter declarer = WhereFilterFactory.getExpression("KeyId < " + VISIBLE_BELOW);
        final WhereFilter respecter = WhereFilterFactory.getExpression("BarrierActivePushdownTest.see(Key)");
        if (!withBarrier) {
            return source.where(Filter.and(declarer, respecter));
        }
        final Object barrier = new Object();
        return source.where(Filter.and(
                declarer.withDeclaredBarriers(barrier), respecter.withRespectedBarriers(barrier)));
    }

    /** Pushdown calls the filter about once per key, so a run that calls it this often evaluated it row by row. */
    private static void assertPushedDown(final String run) {
        assertTrue(run + " evaluated the filter row by row, so it did not reach the tier: " + CALLS.get() + " calls",
                CALLS.get() < ROWS / 100);
    }

    private static void check(final Table source) {
        // Evaluated row by row, with no structure to push down to.
        final Table expected = data().where("KeyId < " + VISIBLE_BELOW).where("BarrierActivePushdownTest.see(Key)");
        assertFalse(expected.isEmpty());

        final Table unbounded = filter(source, false);
        assertPushedDown("unbounded");
        final Set<String> seenUnbounded = Set.copyOf(SEEN);
        final Table bounded = filter(source, true);
        assertPushedDown("bounded");
        final Set<String> seenBounded = Set.copyOf(SEEN);

        assertTableEquals(expected, unbounded);
        assertTableEquals(expected, bounded);
        // Unbounded pushdown shows the filter keys its declarer removed; that is what bounding guards against.
        assertFalse("unbounded pushdown showed only visible keys: " + seenUnbounded,
                visibleKeys().containsAll(seenUnbounded));
        assertFalse(seenBounded.isEmpty());
        assertTrue("bounded pushdown showed keys the declarer removed: " + seenBounded,
                visibleKeys().containsAll(seenBounded));
    }

    @Test
    public void inMemoryDataIndex() {
        final Table source = data();
        // where() uses only an index whose table is already computed. DataIndexer holds the index weakly, so it is
        // held here until the check is done.
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, "Key");
        dataIndex.table();
        check(source);
        Reference.reachabilityFence(dataIndex);
    }

    @Test
    public void parquetDictionary() {
        final File file = new File(tmp.getRoot(), "dictionary.parquet");
        ParquetTools.writeTable(data(), file.getPath());
        check(ParquetTools.readTable(file.getPath()));
    }

    @Test
    public void parquetDictionaryMultipleRowGroups() {
        // Row groups after the first start at a row key offset, which the dictionary scan shifts away.
        final File file = new File(tmp.getRoot(), "dictionary-row-groups.parquet");
        ParquetTools.writeTable(data(), file.getPath(), ParquetInstructions.builder()
                .setRowGroupInfo(RowGroupInfo.maxRows(ROWS / 4))
                .build());
        check(ParquetTools.readTable(file.getPath()));
    }

    @Test
    public void parquetDataIndexMultipleRowGroups() {
        // Several row groups leave gaps in the location's row keys, so the index's positions need converting.
        final File file = new File(tmp.getRoot(), "indexed-row-groups.parquet");
        ParquetTools.writeTable(data(), file.getPath(), ParquetInstructions.builder()
                .addIndexColumns("Key")
                .setMaximumDictionaryKeys(0)
                .setRowGroupInfo(RowGroupInfo.maxRows(ROWS / 4))
                .build());
        final Table source = ParquetTools.readTable(file.getPath());
        // Breaks if row groups stop leaving gaps, which would leave the positional index untested.
        assertFalse("sanity: row groups leave gaps in the row keys", source.getRowSet().isFlat());
        check(source);
    }

    @Test
    public void parquetDataIndex() {
        final File file = new File(tmp.getRoot(), "indexed.parquet");
        ParquetTools.writeTable(data(), file.getPath(), ParquetInstructions.builder()
                .addIndexColumns("Key")
                .setMaximumDictionaryKeys(0)
                .build());
        check(ParquetTools.readTable(file.getPath()));
    }
}
