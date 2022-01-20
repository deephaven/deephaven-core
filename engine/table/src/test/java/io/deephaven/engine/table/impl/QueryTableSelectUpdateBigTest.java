package io.deephaven.engine.table.impl;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTableTestBase.ListenerWithGlobals;
import io.deephaven.engine.table.impl.QueryTableTestBase.TableComparator;
import io.deephaven.engine.table.impl.select.DhFormulaColumn;
import io.deephaven.engine.table.impl.select.FormulaCompilationException;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

/**
 * Test QueryTable select and update operations.
 */
@Category(OutOfBandTest.class)
public class QueryTableSelectUpdateBigTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void test2DSelect() {
        final Table input = emptyTable(3_000_000_000L).updateView("A=(byte)7").where("ii >= 10");
        final Table selected = input.select();
        assertTableEquals(input, selected);
    }
}
