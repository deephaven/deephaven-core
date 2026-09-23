//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
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
        assertNotNull(filter.getFailoverFilterIfCached());
        return filter;
    }

    @Test
    public void copyOfFailoverFilterFailsOverToo() {
        final Table table = data();
        final MatchFilter copy = (MatchFilter) failoverInitializedFilter(table).copy();

        assertNotNull(copy.getFailoverFilterIfCached());
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

        assertNotNull(copy.getFailoverFilterIfCached());
        assertTableEquals(table.where(failoverInitializedFilter(table)), table.where(copy));
    }
}
