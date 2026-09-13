//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
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
 * DH-23557 finding 6: a filter combining a renamed column with an un-renamed one, over a {@link DeferredViewTable},
 * failed with an internal assertion:
 *
 * <pre>
 * io.deephaven.base.verify.AssertionFailure:
 *     Assertion failed: asserted newName != null, instead newName == null.
 * </pre>
 *
 * <p>
 * {@code DeferredViewTable.getFilters} splits a filter into a pre-view part, to be pushed below the view, and a
 * post-view part. For the pre-view part it builds {@code myRenames} from the filter's columns
 * {@code .filter(renames::containsKey)} -- so the map holds only the columns that are actually renamed, and a filter
 * column that passes through unchanged is simply absent.
 *
 * <p>
 * The two consumers of that map disagreed about what an absent key means. {@code AbstractConditionFilter} reads it with
 * {@code getOrDefault(name, name)}, treating absent as unchanged, so {@code ConditionFilter.renameFilter} was fine with
 * a partial map. {@code MatchFilter.renameFilter} instead asserted that its column was present. A disjunction reached
 * both: {@code visit(DisjunctiveFilter)} recurses into each sub-filter with the <em>same</em> map, computed across the
 * whole disjunction's columns, so the sub-filter on the un-renamed column got a map that named only the other column.
 *
 * <p>
 * Fuzzer case seed {@code -4715342832495625892L}, filter {@code or(isNotNull(Col1_r), isNull(Col2))} with {@code Col1}
 * renamed to {@code Col1_r}; 6 of the 36 failures in the run that found it.
 *
 * @see MatchFilter#renameFilter
 */
public class MatchFilterPartialRenameTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    /**
     * Build a {@code DeferredViewTable} whose view renames {@code Value} to {@code Renamed} and passes {@code Other}
     * through unchanged, so a filter over both columns yields a partial rename map.
     */
    private static Table deferredRenamingView() {
        final Table source = TableTools.newTable(
                TableTools.longCol("Value", 1L, 2L, io.deephaven.util.QueryConstants.NULL_LONG),
                TableTools.stringCol("Other", "a", null, "c"));
        final TableDefinition resultDefinition = TableDefinition.of(
                ColumnDefinition.ofLong("Renamed"),
                ColumnDefinition.ofString("Other"));
        final SelectColumn[] viewColumns = SelectColumn.from(
                Selectable.parse("Renamed = Value"),
                Selectable.parse("Other"));
        return new DeferredViewTable(
                resultDefinition,
                "deferredRenamingView",
                new DeferredViewTable.TableReference(source),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                viewColumns,
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);
    }

    private static final Filter RENAMED_NOT_NULL = Filter.isNotNull(ColumnName.of("Renamed"));
    private static final Filter OTHER_IS_NULL = Filter.isNull(ColumnName.of("Other"));

    /**
     * The reported case: a disjunction of a match filter on a renamed column and one on an un-renamed column.
     *
     * <p>
     * Built with the declarative {@link Filter} API rather than from text, as the fuzzer does. Text such as
     * {@code "a || b"} parses to a single {@code ConditionFilter}, which tolerated the partial map all along; only a
     * real {@code DisjunctiveFilter} of {@code MatchFilter}s reaches the defect.
     */
    @Test
    public void disjunctionOfRenamedAndUnrenamedMatchFilters() {
        // Renamed is non-null in rows 0 and 1; Other is null in row 1. The union is rows 0 and 1.
        assertEquals(2, deferredRenamingView().where(Filter.or(RENAMED_NOT_NULL, OTHER_IS_NULL)).size());
    }

    /** The mirror image: the un-renamed column first, so the absent key is the first one visited. */
    @Test
    public void disjunctionWithUnrenamedColumnFirst() {
        assertEquals(2, deferredRenamingView().where(Filter.or(OTHER_IS_NULL, RENAMED_NOT_NULL)).size());
    }

    /**
     * A control: a top-level conjunction did <em>not</em> fail before the fix. {@code where} flattens
     * {@code Filter.and(...)} into separate top-level filters, each of which gets its own map, total over its own one
     * column. Only a conjunction nested inside something else reaches the recursive visitor -- see
     * {@link #nestedDisjunctionInsideConjunction}.
     */
    @Test
    public void conjunctionOfRenamedAndUnrenamedMatchFilters() {
        // Renamed non-null and Other null: row 1 only.
        assertEquals(1, deferredRenamingView().where(Filter.and(RENAMED_NOT_NULL, OTHER_IS_NULL)).size());
    }

    /** Nested disjunction inside conjunction, to cover the recursion more than one level deep. */
    @Test
    public void nestedDisjunctionInsideConjunction() {
        assertEquals(2, deferredRenamingView().where(Filter.and(
                Filter.or(RENAMED_NOT_NULL, OTHER_IS_NULL),
                FilterComparison.neq(ColumnName.of("Renamed"), Literal.of(999L)))).size());
    }

    /** Equality match filters, not just null checks: those take the values rather than strValues branch. */
    @Test
    public void disjunctionOfEqualityMatchFilters() {
        assertEquals(2, deferredRenamingView().where(Filter.or(
                FilterComparison.eq(ColumnName.of("Renamed"), Literal.of(1L)),
                FilterComparison.eq(ColumnName.of("Other"), Literal.of("c")))).size());
    }

    /** A filter touching only the un-renamed column yields an empty map and was always pushed through unchanged. */
    @Test
    public void filterOnUnrenamedColumnAlone() {
        assertEquals(1, deferredRenamingView().where(OTHER_IS_NULL).size());
    }

    /** A filter touching only the renamed column has a total map and always worked. */
    @Test
    public void filterOnRenamedColumnAlone() {
        assertEquals(2, deferredRenamingView().where(RENAMED_NOT_NULL).size());
    }

    /** The results must match the same filters applied after the view is materialized. */
    @Test
    public void agreesWithTheMaterializedView() {
        final Filter[] filters = {
                Filter.or(RENAMED_NOT_NULL, OTHER_IS_NULL),
                Filter.or(OTHER_IS_NULL, RENAMED_NOT_NULL),
                Filter.and(RENAMED_NOT_NULL, OTHER_IS_NULL),
                Filter.or(FilterComparison.eq(ColumnName.of("Renamed"), Literal.of(1L)),
                        FilterComparison.eq(ColumnName.of("Other"), Literal.of("c"))),
                Filter.or(FilterComparison.eq(ColumnName.of("Renamed"), Literal.of(2L)),
                        FilterComparison.eq(ColumnName.of("Other"), Literal.of("a")))};
        for (final Filter filter : filters) {
            final long deferred = deferredRenamingView().where(filter).size();
            final long materialized = deferredRenamingView().select().where(filter).size();
            assertEquals(filter.toString(), materialized, deferred);
        }
    }
}
