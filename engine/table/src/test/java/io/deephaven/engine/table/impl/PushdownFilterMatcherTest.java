//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertThrows;

/**
 * Tests for {@link PushdownFilterMatcher#getPushdownFilterMatcher(WhereFilter, List)}, in particular the requirement
 * that {@code filterSources} be parallel to {@code filter.getColumns()} (DH-23106).
 */
public class PushdownFilterMatcherTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private Table table;
    private WhereFilter singleColumnFilter;
    private WhereFilter multiColumnFilter;

    @Before
    public void setUp() {
        table = TableTools.newTable(
                stringCol("X", "A", "B", "C"),
                stringCol("Y", "D", "E", "F"));
        singleColumnFilter = new MatchFilter(MatchOptions.REGULAR, "X", "A");
        multiColumnFilter = ConjunctiveFilter.of(
                new MatchFilter(MatchOptions.REGULAR, "X", "A"),
                new MatchFilter(MatchOptions.REGULAR, "Y", "D"));
        singleColumnFilter.init(table.getDefinition());
        multiColumnFilter.init(table.getDefinition());
    }

    @Test
    public void testParallelSourcesAccepted() {
        final ColumnSource<?> csX = table.getColumnSource("X");
        final ColumnSource<?> csY = table.getColumnSource("Y");
        // Correctly-parallel lists must not throw, whatever matcher (or null) they resolve to.
        PushdownFilterMatcher.getPushdownFilterMatcher(singleColumnFilter, List.of(csX));
        PushdownFilterMatcher.getPushdownFilterMatcher(multiColumnFilter, List.of(csX, csY));
    }

    @Test
    public void testSingleColumnFilterWithEmptySourcesThrows() {
        // Before DH-23106 this threw IndexOutOfBoundsException from filterSources.get(0)
        assertThrows(AssertionFailure.class, () -> PushdownFilterMatcher.getPushdownFilterMatcher(
                singleColumnFilter, Collections.emptyList()));
    }

    @Test
    public void testMultiColumnFilterWithPartialSourcesThrows() {
        // Before DH-23106 a partial source list was silently passed to PushdownPredicateManager.getSharedPPM,
        // potentially selecting a shared PPM that does not cover all of the filter's columns
        final ColumnSource<?> csX = table.getColumnSource("X");
        assertThrows(AssertionFailure.class, () -> PushdownFilterMatcher.getPushdownFilterMatcher(
                multiColumnFilter, List.of(csX)));
    }
}
