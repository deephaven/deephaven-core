//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.type.ArrayTypeUtils;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * DH-23557 finding 7: a formula filter pushed through more than one level of renaming deferred view lost every renaming
 * but the last, and failed to compile:
 *
 * <pre>
 * FormulaCompilationException: Formula compilation error for: Renamed != null &amp;&amp; Dup != null &amp;&amp; Renamed == Dup
 *     caused by: QueryLanguageParser$ParserResolutionFailure: Cannot find variable or class Dup
 * </pre>
 *
 * <p>
 * {@code ConditionFilter.renameFilter} leaves the formula text alone and carries the renaming in
 * {@code AbstractConditionFilter.outerToInnerNames}, resolved when the filter initializes. It built the copy with
 * {@code new ConditionFilter(formula, renames)} -- <em>replacing</em> the map rather than folding the new mapping into
 * it. A filter is renamed once per deferred view it is pushed through, and
 * {@code DeferredViewTable.splitAndApplyFilters} recurses through nested views via {@code CopiedTableReference}, so
 * with two levels the first level's mapping was discarded and its formula variable named nothing.
 *
 * <p>
 * Two levels arise from something as ordinary as a rename followed by a duplicating {@code updateView}, which is
 * exactly what the fuzzer generated: {@code renameColumns("Col0_r = Col0")} then
 * {@code updateView("dup0_Col0_r = Col0_r")}.
 *
 * <p>
 * Fuzzer case seed {@code 7177646707619336702L}; 6 of the 36 failures in the run that found it.
 *
 * @see io.deephaven.engine.table.impl.select.AbstractConditionFilter#composeRenames
 */
public class NestedDeferredViewRenameTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    /** A deferred view that renames {@code Value} to {@code Renamed}: one level of renaming. */
    private static Table deferredRenamed() {
        final Table source = TableTools.newTable(
                TableTools.intCol("Value", 1, 2, io.deephaven.util.QueryConstants.NULL_INT),
                TableTools.stringCol("Other", "a", "b", "c"));
        return new DeferredViewTable(
                TableDefinition.of(ColumnDefinition.ofInt("Renamed"), ColumnDefinition.ofString("Other")),
                "deferredRenamed",
                new DeferredViewTable.TableReference(source),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.from(Selectable.parse("Renamed = Value"), Selectable.parse("Other")),
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);
    }

    /** Two levels: the rename above, then a duplicating view on top of it. */
    private static Table deferredRenamedWithDuplicate() {
        return deferredRenamed().updateView("Dup = Renamed");
    }

    /** The reported case. */
    @Test
    public void formulaOverRenamedAndDuplicatedColumns() {
        assertEquals(2, deferredRenamedWithDuplicate()
                .where("Renamed != null && Dup != null && Renamed == Dup").size());
    }

    /** The duplicate alone, with the original never mentioned. */
    @Test
    public void formulaOverTheDuplicateOnly() {
        assertEquals(1, deferredRenamedWithDuplicate().where("Dup == 2").size());
    }

    /** Three levels of renaming, to show the composition chains rather than merely handling two. */
    @Test
    public void formulaOverThreeLevelsOfRenaming() {
        final Table threeLevels = deferredRenamedWithDuplicate().updateView("Dup2 = Dup");
        assertEquals(2, threeLevels.where("Renamed != null && Dup2 != null && Renamed == Dup2").size());
        assertEquals(1, threeLevels.where("Dup2 == 1").size());
    }

    /** A formula mixing a renamed column, its duplicate, and a column that was never renamed. */
    @Test
    public void formulaMixingRenamedDuplicateAndUntouchedColumns() {
        assertEquals(1, deferredRenamedWithDuplicate()
                .where("Renamed == Dup && Other == `a`").size());
    }

    /** One level of renaming always worked; keep it covered so the composition does not break the simple case. */
    @Test
    public void formulaOverASingleLevelOfRenaming() {
        assertEquals(2, deferredRenamed().where("Renamed != null && Renamed > 0").size());
    }

    /** Every filter must agree with the same filter applied after the views are materialized. */
    @Test
    public void agreesWithTheMaterializedViews() {
        for (final String filter : new String[] {
                "Renamed != null && Dup != null && Renamed == Dup",
                "Dup == 2",
                "Renamed == Dup && Other == `a`",
                "Dup != null && Dup < 2",
                "Renamed == null || Dup == 1"}) {
            final long deferred = deferredRenamedWithDuplicate().where(filter).size();
            final long materialized = deferredRenamedWithDuplicate().select().where(filter).size();
            assertEquals(filter, materialized, deferred);
        }
    }
}
