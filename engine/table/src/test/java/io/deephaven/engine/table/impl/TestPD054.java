//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConditionFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reproducer for <b>PD-054 (P2)</b> — {@code ConditionFilter.getNumInputsUsed()} dereferenced a null
 * {@code usedInputs}, so building a {@link BasePushdownFilterContextImpl} for a vectorization-initialized filter threw
 * {@link NullPointerException}.
 * <p>
 * {@code AbstractConditionFilter.checkAndInitializeVectorization} sets {@code initialized = true} for a vectorizable
 * Python function having installed a chunk filter, <i>without</i> running {@code ConditionFilter.getClassBody} — the
 * only place {@code usedInputs} was ever assigned. {@code BasePushdownFilterContextImpl}'s constructor then calls
 * {@code getNumInputsUsed()} while deciding {@code supportsChunkFiltering}, and dereferenced null.
 * <p>
 * The hole was masked by {@code ConditionFilter.permitParallelization()}, which short-circuits false for Python on a
 * non-free-threaded interpreter and so routes such filters to the stateful, non-pushdown collection — note that the
 * guard itself reads {@code usedInputs}, and was safe only by {@code ||} evaluation order. Free-threaded Python flips
 * that gate, at which point any vectorized Python filter over a pushdown-capable source would have crashed during
 * context construction, failing the whole {@code where()} per PD-040.
 * <p>
 * The vectorized path needs a Python interpreter, so this test reproduces the <i>state</i> it leaves behind — a
 * {@code ConditionFilter} that permits parallelization but whose inputs were never determined — via
 * {@code ConditionFilter.createStateless}, whose {@code permitParallelization()} returns true unconditionally, exactly
 * as free-threaded Python would.
 * <p>
 * <b>Expected after a fix:</b> the context is built and reports that it cannot chunk-filter. <b>Before the fix</b> the
 * constructor threw NPE.
 */
public class TestPD054 {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * Root cause: the input count must be answerable whenever the filter claims to be initialized, and a filter whose
     * inputs were never determined must report zero rather than dereference null.
     */
    @Test
    public void getNumInputsUsedIsAnswerableWithoutClassBody() {
        final ConditionFilter filter = (ConditionFilter) ConditionFilter.createStateless("X > 5");

        assertEquals("a filter whose inputs were never determined uses no inputs", 0, filter.getNumInputsUsed());
    }

    /**
     * The consumer: building the pushdown context must degrade to "not chunk-filterable" rather than throwing, because
     * an exception here fails the entire {@code where()}.
     */
    @Test
    public void pushdownContextDegradesRatherThanThrowing() {
        final Table table = TableTools.newTable(intCol("X", 1, 5, 9));
        final ColumnSource<?> columnSource = table.getColumnSource("X");

        final WhereFilter filter = ConditionFilter.createStateless("X > 5");
        assertTrue("createStateless must permit parallelization, as free-threaded Python would",
                filter.permitParallelization());

        // Deliberately not init'ed: this is the state checkAndInitializeVectorization leaves behind -- marked
        // initialized, with a chunk filter installed, but usedInputs never assigned.
        final BasePushdownFilterContextImpl context =
                new BasePushdownFilterContextImpl(filter, List.of(columnSource));

        assertFalse("a filter with an undetermined input count is not usable as a single-column chunk filter",
                context.supportsChunkFiltering());
    }
}
