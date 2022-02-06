/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.util.ExceptionDetails;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.function.Supplier;

abstract public class RefreshingTableTestCase extends BaseArrayTestCase implements UpdateErrorReporter {
    public static boolean printTableUpdates = Configuration.getInstance()
            .getBooleanForClassWithDefault(RefreshingTableTestCase.class, "printTableUpdates", false);
    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(RefreshingTableTestCase.class, "CompilerTools.logEnabled", false);

    private boolean oldMemoize;
    private UpdateErrorReporter oldReporter;
    private boolean expectError = false;
    private SafeCloseable livenessScopeCloseable;
    private QueryScope originalQueryScope;
    private boolean oldLogEnabled;
    private boolean oldCheckLtm;

    List<Throwable> errors;

    public static int scaleToDesiredTestLength(final int maxIter) {
        return TstUtils.scaleToDesiredTestLength(maxIter);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();
        oldMemoize = QueryTable.setMemoizeResults(false);
        oldReporter = AsyncClientErrorNotifier.setReporter(this);
        errors = null;
        livenessScopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);
        originalQueryScope = QueryScope.getScope();

        oldLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
        oldCheckLtm = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(false);
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    protected void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
        UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheckLtm);
        CompilerTools.setLogEnabled(oldLogEnabled);

        QueryScope.setScope(originalQueryScope);
        livenessScopeCloseable.close();
        AsyncClientErrorNotifier.setReporter(oldReporter);
        QueryTable.setMemoizeResults(oldMemoize);
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);

        super.tearDown();
    }

    @Override
    public void reportUpdateError(Throwable t) throws IOException {
        if (!expectError) {
            System.err.println("Received error notification: " + new ExceptionDetails(t).getFullStackTrace());
            TestCase.fail(t.getMessage());
        }
        if (errors == null) {
            errors = new ArrayList<>();
        }
        errors.add(t);
    }

    public List<Throwable> getUpdateErrors() {
        if (errors == null) {
            return Collections.emptyList();
        }
        return errors;
    }

    public boolean getExpectError() {
        return expectError;
    }

    public void setExpectError(boolean expectError) {
        this.expectError = expectError;
    }

    <T> T allowingError(Supplier<T> function, Predicate<List<Throwable>> errorsAcceptable) {
        final boolean original = getExpectError();
        T retval;
        try {
            setExpectError(true);
            retval = function.get();
        } finally {
            setExpectError(original);
        }
        if (!errorsAcceptable.test(errors)) {
            TestCase.fail("Unacceptable errors: " + errors);
        }
        return retval;
    }

    public void allowingError(Runnable function, Predicate<List<Throwable>> errorsAcceptable) {
        allowingError(() -> {
            function.run();
            return true;
        }, errorsAcceptable);
    }

    protected static void simulateShiftAwareStep(int targetUpdateSize, Random random, QueryTable table,
            TstUtils.ColumnInfo[] columnInfo, EvalNuggetInterface[] en) {
        simulateShiftAwareStep("", targetUpdateSize, random, table, columnInfo, en);
    }

    public static void simulateShiftAwareStep(final String ctxt, int targetUpdateSize, Random random, QueryTable table,
            TstUtils.ColumnInfo[] columnInfo, EvalNuggetInterface[] en) {
        simulateShiftAwareStep(GenerateTableUpdates.DEFAULT_PROFILE, ctxt, targetUpdateSize, random, table, columnInfo,
                en);
    }

    protected static void simulateShiftAwareStep(final GenerateTableUpdates.SimulationProfile simulationProfile,
            final String ctxt, int targetUpdateSize, Random random, QueryTable table, TstUtils.ColumnInfo[] columnInfo,
            EvalNuggetInterface[] en) {
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates
                .generateShiftAwareTableUpdates(simulationProfile, targetUpdateSize, random, table, columnInfo));
        TstUtils.validate(ctxt, en);
        // The EvalNugget test cases end up generating very big listener DAGs, for at each step we create a brand new
        // live incarnation of the table. This can make debugging a bit awkward, so sometimes it is convenient to
        // prune the tree after each validation. The reason not to do it, however, is that this will sometimes expose
        // bugs with shared indices getting updated.
        // System.gc();
    }

    class ErrorExpectation implements Closeable {
        final boolean originalExpectError;

        ErrorExpectation() {
            originalExpectError = expectError;
            expectError = true;
        }

        @Override
        public void close() {
            expectError = originalExpectError;
        }
    }
}
