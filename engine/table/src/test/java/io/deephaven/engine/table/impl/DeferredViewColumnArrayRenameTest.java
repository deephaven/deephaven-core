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
 * DH-23557 finding 8: a filter naming both a renamed column and its column-array form crashed the deferred-view filter
 * split:
 *
 * <pre>
 * java.lang.IllegalStateException: Duplicate key Renamed (attempted merging values Value and Value)
 *     at java.util.stream.Collectors.duplicateKeyException
 * </pre>
 *
 * <p>
 * {@code DeferredViewTable.applyFilterRenamings} builds the filter's rename map from
 * {@code Stream.of(filter.getColumns(), filter.getColumnArrays()).flatMap(...)}. A filter such as
 * {@code "Renamed == Renamed_[0]"} reports {@code Renamed} in <em>both</em> lists -- once as a column and once as the
 * column backing {@code Renamed_} -- and {@code Collectors.toMap} has no merge function, so the second occurrence
 * threw.
 *
 * <p>
 * The two entries always map the column to the same inner name, as the exception's own "attempted merging values Value
 * and Value" shows, so nothing was ambiguous; the duplicate was purely a crash.
 *
 * <p>
 * Found while writing {@code NestedDeferredViewRenameTest} for finding 7, which is a different defect in the same
 * method.
 */
public class DeferredViewColumnArrayRenameTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    /** A deferred view renaming {@code Value} to {@code Renamed}, so filter columns need mapping. */
    private static Table deferredRenamed() {
        final Table source = TableTools.newTable(
                TableTools.intCol("Value", 5, 6, 7),
                TableTools.stringCol("Other", "a", "b", "c"));
        return new DeferredViewTable(
                TableDefinition.of(ColumnDefinition.ofInt("Renamed"), ColumnDefinition.ofString("Other")),
                "deferredRenamed",
                new DeferredViewTable.TableReference(source),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                SelectColumn.from(Selectable.parse("Renamed = Value"), Selectable.parse("Other")),
                WhereFilter.ZERO_LENGTH_WHERE_FILTER_ARRAY);
    }

    /** The reported case: the same renamed column used as a value and as an array. */
    @Test
    public void columnAndItsArrayForm() {
        // Renamed_[0] is 5, so only the first row matches.
        assertEquals(1, deferredRenamed().where("Renamed == Renamed_[0]").size());
    }

    /** The array form alone, which names the column only via getColumnArrays. */
    @Test
    public void arrayFormAlone() {
        assertEquals(3, deferredRenamed().where("Renamed_[0] == 5").size());
    }

    /** Two renamed columns, each used both ways, so both would duplicate. */
    @Test
    public void twoColumnsBothUsedBothWays() {
        assertEquals(1, deferredRenamed()
                .where("Renamed == Renamed_[0] && Other == Other_[0]").size());
    }

    /** A renamed column used both ways alongside an un-renamed one. */
    @Test
    public void columnAndArrayFormWithUntouchedColumn() {
        assertEquals(1, deferredRenamed().where("Renamed == Renamed_[0] && Other == `a`").size());
    }

    /** Offsets other than zero, to be sure the array is the real column and not a stale copy. */
    @Test
    public void arrayFormAtANonZeroOffset() {
        assertEquals(1, deferredRenamed().where("Renamed == Renamed_[2]").size());
    }

    /** Results must match the same filters applied after the view is materialized. */
    @Test
    public void agreesWithTheMaterializedView() {
        for (final String filter : new String[] {
                "Renamed == Renamed_[0]",
                "Renamed_[0] == 5",
                "Renamed == Renamed_[0] && Other == Other_[0]",
                "Renamed == Renamed_[2]",
                "Renamed > Renamed_[0]"}) {
            final long deferred = deferredRenamed().where(filter).size();
            final long materialized = deferredRenamed().select().where(filter).size();
            assertEquals(filter, materialized, deferred);
        }
    }
}
