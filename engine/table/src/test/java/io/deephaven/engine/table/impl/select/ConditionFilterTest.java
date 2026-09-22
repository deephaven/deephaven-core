//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ConditionFilter}.
 */
public class ConditionFilterTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private Table table;

    @Before
    public void setUp() {
        table = TableTools.newTable(intCol("X", 1, 5, 9), intCol("Y", 2, 4, 6));
    }

    private static ConditionFilter initialized(final String formula, final Table table) {
        final ConditionFilter filter = (ConditionFilter) ConditionFilter.createConditionFilter(formula);
        filter.init(table.getDefinition());
        return filter;
    }

    /**
     * The input count is derived from the analysis {@code init()} performs, so it is defined before the generated
     * kernel exists and must not fail on a filter whose inputs have not been determined yet.
     */
    @Test
    public void testGetNumInputsUsedBeforeInit() {
        final ConditionFilter filter = (ConditionFilter) ConditionFilter.createConditionFilter("X > 5");
        assertEquals("no inputs are known before init", 0, filter.getNumInputsUsed());
    }

    @Test
    public void testGetNumInputsUsedCountsColumns() {
        assertEquals(1, initialized("X > 5", table).getNumInputsUsed());
        assertEquals(2, initialized("X > 5 && Y < 5", table).getNumInputsUsed());
        assertEquals("a column referenced twice is one input", 1, initialized("X > 5 && X < 9", table)
                .getNumInputsUsed());
    }

    @Test
    public void testGetNumInputsUsedCountsVirtualRowVariables() {
        assertEquals("i is an input", 1, initialized("i > 0", table).getNumInputsUsed());
        assertEquals("a column plus ii is two inputs", 2, initialized("X > 5 && ii > 0", table).getNumInputsUsed());
        assertEquals("a column plus i, ii and k is four inputs", 4,
                initialized("X > 5 && i > 0 && ii > 0 && k > 0", table).getNumInputsUsed());
    }
}
