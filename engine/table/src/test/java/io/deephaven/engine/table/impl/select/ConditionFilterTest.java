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

import java.lang.reflect.Field;

import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testGetNumInputsUsedCountsColumns() {
        assertEquals(1, initialized("X > 5", table).getNumInputsUsed());
        assertEquals(2, initialized("X > 5 && Y < 5", table).getNumInputsUsed());
        assertEquals("a column referenced twice is one input", 1, initialized("X > 5 && X < 9", table)
                .getNumInputsUsed());
    }

    /**
     * A copy of a filter initialized from a vectorizable Python function must still know it is a Python filter, so that
     * its {@code permitParallelization()} keeps consulting the interpreter's threading model. That method needs a
     * running interpreter, which this module's tests do not have, so the marker itself is checked reflectively.
     */
    @Test
    public void testCopyPreservesPythonFilterMarker() throws Exception {
        final ConditionFilter filter = initialized("X > 5", table);
        // Stand in for checkAndInitializeVectorization, which installs the vectorized function through this hook; the
        // filter's own generated kernel serves as the installed Filter here, only the marker matters.
        filter.setPythonFilter(filter.getFilter(table, table.getRowSet()));
        assertTrue("sanity: the original is marked as a Python filter", isPythonFilter(filter));

        assertTrue("the copy must remain a Python filter", isPythonFilter(filter.copy()));
    }

    private static boolean isPythonFilter(final ConditionFilter filter) throws Exception {
        final Field field = ConditionFilter.class.getDeclaredField("pythonFilter");
        field.setAccessible(true);
        return field.getBoolean(filter);
    }

    @Test
    public void testGetNumInputsUsedCountsVirtualRowVariables() {
        assertEquals("i is an input", 1, initialized("i > 0", table).getNumInputsUsed());
        assertEquals("a column plus ii is two inputs", 2, initialized("X > 5 && ii > 0", table).getNumInputsUsed());
        assertEquals("a column plus i, ii and k is four inputs", 4,
                initialized("X > 5 && i > 0 && ii > 0 && k > 0", table).getNumInputsUsed());
    }
}
