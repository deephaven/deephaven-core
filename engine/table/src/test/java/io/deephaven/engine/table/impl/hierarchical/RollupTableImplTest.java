//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RollupTableImplTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * Filtering a rollup on its group-by columns filters its base level's row lookup, which has no update cycle step to
     * report, since the filtered rows change with the filter's result rather than the aggregation's hash table.
     */
    @Test
    public void testFilteredBaseLevelLookupHasNoUpdateCycleStep() {
        final Table source = TableTools.emptyTable(12).update("Key = (int) (ii % 3)", "Value = ii");
        final RollupTableImpl rollup =
                (RollupTableImpl) source.rollup(List.of(Aggregation.AggSum("Value")), false, "Key");
        final AggregationRowLookup lookup = rollup.baseLevelRowLookup();
        assertTrue(lookup.get(2) >= 0);
        // the aggregation of a static source never changes
        assertEquals(QueryConstants.NULL_LONG, lookup.updateCycleStep());

        final RollupTableImpl filtered =
                (RollupTableImpl) rollup.withFilter(WhereFilterFactory.getExpression("Key = 1"));
        final AggregationRowLookup filteredLookup = filtered.baseLevelRowLookup();
        assertTrue(filteredLookup.get(1) >= 0);
        assertEquals(AggregationRowLookup.DEFAULT_UNKNOWN_ROW, filteredLookup.get(2));
        assertThrows(UnsupportedOperationException.class, filteredLookup::updateCycleStep);
    }
}
