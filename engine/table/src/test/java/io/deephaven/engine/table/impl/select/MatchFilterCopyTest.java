//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MatchFilterCopyTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static Table data() {
        return newTable(intCol("X", 1, 2, 3), intCol("Y", 1, 3, 3));
    }

    /** {@code X == Y} cannot convert {@code Y} to a value, so it fails over to a {@link ConditionFilter}. */
    private static MatchFilter failoverInitializedFilter(final Table table) {
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == Y");
        filter.init(table.getDefinition());
        assertNotNull(filter.getFailoverFilter());
        return filter;
    }

    @Test
    public void copyOfFailoverFilterFailsOverToo() {
        final Table table = data();
        final MatchFilter copy = (MatchFilter) failoverInitializedFilter(table).copy();

        assertNotNull(copy.getFailoverFilter());
        assertTrue(MatchFilter.extractMatchFilter(copy).isEmpty());
        assertEquals(List.of("X", "Y"), copy.getColumns());
    }

    @Test
    public void copyOfFailoverFilterFiltersLikeTheOriginal() {
        final Table table = data();

        assertTableEquals(
                newTable(intCol("X", 1, 3), intCol("Y", 1, 3)),
                table.where(failoverInitializedFilter(table).copy()));
    }

    @Test
    public void copyOfConvertedFilterKeepsItsValues() {
        final Table table = data();
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X in 1, 3");
        filter.init(table.getDefinition());
        final MatchFilter copy = (MatchFilter) filter.copy();

        assertArrayEquals(filter.getValues(), copy.getValues());
        assertTrue(MatchFilter.extractMatchFilter(copy).isPresent());
        assertTableEquals(table.where(filter), table.where(copy));
    }

    @Test
    public void copyOfUninitializedFilterInitializesIndependently() {
        final Table table = data();
        final MatchFilter copy = (MatchFilter) WhereFilterFactory.getExpression("X == Y").copy();
        copy.init(table.getDefinition());

        assertNotNull(copy.getFailoverFilter());
        assertTableEquals(table.where(failoverInitializedFilter(table)), table.where(copy));
    }

    @Test
    public void copyConvertsWhenAnEarlierCopyFailedOver() {
        QueryScope.addParam("matchFilterCopyVal", 1);
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("X == matchFilterCopyVal");

        // where the value names a column, the column takes precedence and this copy fails over
        final MatchFilter columnCopy = (MatchFilter) filter.copy();
        columnCopy.init(TableDefinition.of(ColumnDefinition.ofInt("X"), ColumnDefinition.ofInt("matchFilterCopyVal")));
        assertNotNull(columnCopy.getFailoverFilter());

        final Table table = data();
        final MatchFilter intCopy = (MatchFilter) filter.copy();
        intCopy.init(table.getDefinition());

        assertNull(intCopy.getFailoverFilter());
        assertTrue(MatchFilter.extractMatchFilter(intCopy).isPresent());
        assertTableEquals(newTable(intCol("X", 1), intCol("Y", 1)), table.where(intCopy));
    }

    @Test
    public void originalStillSwapsAfterItsCopyDid() {
        QueryScope.addParam("matchFilterSwapVal", 1);
        // parses with the column and variable swapped: column "matchFilterSwapVal", value "X"
        final MatchFilter filter = (MatchFilter) WhereFilterFactory.getExpression("matchFilterSwapVal in X");
        final MatchFilter copy = (MatchFilter) filter.copy();
        final Table table = data();

        // The copy shares the original's string values. Were init to swap them in place, the original would be left
        // with neither name a column, and its own init would throw.
        copy.init(table.getDefinition());
        assertEquals(List.of("X"), copy.getColumns());

        filter.init(table.getDefinition());
        assertEquals(List.of("X"), filter.getColumns());
        assertTableEquals(newTable(intCol("X", 1), intCol("Y", 1)), table.where(filter));
    }

    @Test
    public void failoverWithRowVariableIsNotPushedToADataIndex() {
        final Table table = newTable(intCol("X", 0, 0, 0, 0, 0, 1, 1, 1, 1, 1));
        // where uses only a data index whose table is cached
        DataIndexer.getOrCreateDataIndex(table, "X").table();

        assertTableEquals(newTable(intCol("X", 0)), table.where("X == i"));
    }
}
