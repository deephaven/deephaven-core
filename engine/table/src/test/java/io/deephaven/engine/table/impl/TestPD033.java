//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reproducer for <b>PD-033 (P0)</b> — {@code hasVirtualRowVariables()} is not propagated by filter wrappers, so a
 * filter using {@code i}/{@code ii}/{@code k} slips into pushdown and is evaluated against the <b>data index table</b>
 * instead of the source table.
 * <p>
 * {@code WhereFilter.hasVirtualRowVariables()} defaults to {@code false} ({@code select/WhereFilter.java:347-349}) and
 * only {@code AbstractConditionFilter} overrides it ({@code :340-342}). {@code ComposedFilter} — the base of
 * {@code ConjunctiveFilter}/{@code DisjunctiveFilter} — and {@code WhereFilterDelegatingBase} — the base of the
 * inverted and barrier wrappers — delegate {@code getColumns()}/{@code getColumnArrays()} but <b>not</b> this.
 * <p>
 * So {@code PushdownFilterMatcher.canPushdownFilter} ({@code :145-150}) accepts a wrapped {@code ii} filter. With a
 * cached data index on the sibling column, {@code WhereListener.extractFilterDataIndexMap} accepts it ({@code :113}),
 * the driver wraps {@code DataIndexPushdownManager} ({@code AbstractFilterExecution.java:516}), and
 * {@code pushdownDataIndex} ({@code dataindex/DataIndexPushdownManager.java:214-256}) applies the composed filter <b>to
 * the index table</b> — where {@code ii} evaluates to index-table positions rather than source positions. Whole groups
 * are classified match/no-match with {@code maybeMatch} empty, so nothing corrects it.
 * <p>
 * Data used: {@code A = ii % 3} over 100 rows, with a data index on {@code A}, filtered with
 * {@code A = 1 || ii % 2 == 0}. The index table has just 3 rows (one per distinct {@code A}), so {@code ii % 2 == 0}
 * evaluated there selects index rows 0 and 2 — i.e. the whole {@code A=0} and {@code A=2} groups — rather than the even
 * source rows.
 * <p>
 * The modulus matters. An earlier attempt at this repro used {@code A = ii % 10}, for which {@code ii % 2 == 0}
 * evaluated on the 10-row index table is <i>equivalent</i> to the source predicate (even {@code ii} ⟺ even {@code A},
 * and index position happens to equal {@code A}) — the defect fires but produces the right answer by coincidence, and
 * the end-to-end tests pass. {@code % 3} breaks that coincidence.
 * <p>
 * <b>Expected after a fix:</b> all four tests below pass. <b>Today all four fail</b>, demonstrating the bug.
 */
public class TestPD033 {

    private static final int TABLE_SIZE = 100;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private boolean savedUseDataIndex;

    @Before
    public void setUp() {
        savedUseDataIndex = QueryTable.USE_DATA_INDEX_FOR_WHERE;
    }

    @After
    public void tearDown() {
        QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndex;
    }

    /** A fresh table each time; only the ones we explicitly index carry a data index. */
    private static QueryTable makeTable() {
        return (QueryTable) testRefreshingTable(RowSetFactory.flat(TABLE_SIZE).toTracking())
                .update("A = (int) (ii % 3)");
    }

    /** {@code A = 1 || ii % 2 == 0} — a disjunction, which {@code where()} cannot split into separate filters. */
    private static Filter virtualRowVariableDisjunction() {
        return Filter.or(RawString.of("A = 1"), RawString.of("ii % 2 == 0"));
    }

    /**
     * Root cause, part 1: {@code ComposedFilter} must report that it uses virtual row variables when a component does.
     */
    @Test
    public void composedFilterReportsVirtualRowVariables() {
        final QueryTable table = makeTable();
        final WhereFilter filter = WhereFilter.of(virtualRowVariableDisjunction());
        filter.init(table.getDefinition());

        assertTrue("a disjunction containing an `ii` filter uses virtual row variables",
                filter.hasVirtualRowVariables());
    }

    /**
     * Root cause, part 2: such a filter must therefore be rejected for pushdown.
     */
    @Test
    public void canPushdownFilterRejectsVirtualRowVariableDisjunction() {
        final QueryTable table = makeTable();
        final WhereFilter filter = WhereFilter.of(virtualRowVariableDisjunction());
        filter.init(table.getDefinition());

        assertFalse("a filter using `ii` must not be pushed down",
                PushdownFilterMatcher.canPushdownFilter(filter));
    }

    /**
     * End-to-end: the same query against an indexed and an unindexed table must agree. {@code USE_DATA_INDEX_FOR_WHERE}
     * is toggled for the oracle so that both sides are otherwise identical.
     */
    @Test
    public void indexedResultMatchesUnindexedOracle() {
        final QueryTable indexedTable = makeTable();
        DataIndexer.getOrCreateDataIndex(indexedTable, "A");

        final Table oracle;
        QueryTable.USE_DATA_INDEX_FOR_WHERE = false;
        try {
            oracle = makeTable().where(virtualRowVariableDisjunction()).coalesce();
        } finally {
            QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndex;
        }

        assertTableEquals(
                // expected: no data-index pushdown, so `ii` means source positions
                oracle,
                // actual: data-index pushdown evaluates `ii` against the 10-row index table
                indexedTable.where(virtualRowVariableDisjunction()).coalesce());
    }

    /**
     * The same defect reached through {@code WhereFilterDelegatingBase} — a barrier wrapper around the disjunction —
     * which likewise fails to propagate {@code hasVirtualRowVariables()}.
     */
    @Test
    public void barrierWrappedIndexedResultMatchesUnindexedOracle() {
        final QueryTable indexedTable = makeTable();
        DataIndexer.getOrCreateDataIndex(indexedTable, "A");

        final Table oracle;
        QueryTable.USE_DATA_INDEX_FOR_WHERE = false;
        try {
            oracle = makeTable()
                    .where(virtualRowVariableDisjunction().withDeclaredBarriers("PD033_BARRIER"))
                    .coalesce();
        } finally {
            QueryTable.USE_DATA_INDEX_FOR_WHERE = savedUseDataIndex;
        }

        assertTableEquals(
                oracle,
                indexedTable
                        .where(virtualRowVariableDisjunction().withDeclaredBarriers("PD033_BARRIER"))
                        .coalesce());
    }
}
