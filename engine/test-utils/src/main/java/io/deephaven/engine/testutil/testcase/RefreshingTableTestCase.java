//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.testcase;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerImpl;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateErrorReporter;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier;
import io.deephaven.engine.table.impl.util.AsyncErrorLogger;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.util.ExceptionDetails;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import junit.framework.TestCase;

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
    private static final boolean ENABLE_QUERY_COMPILER_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(RefreshingTableTestCase.class, "QueryCompile.logEnabled", false);

    private ProcessEnvironment oldProcessEnvironment;
    private boolean oldMemoize;
    private UpdateErrorReporter oldReporter;
    private boolean expectError = false;
    private SafeCloseable livenessScopeCloseable;
    private boolean oldLogEnabled;
    private boolean oldSerialSafe;
    private SafeCloseable executionContext;

    List<Throwable> errors;

    public static int scaleToDesiredTestLength(final int maxIter) {
        return TstUtils.scaleToDesiredTestLength(maxIter);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        oldProcessEnvironment = ProcessEnvironment.tryGet();
        ProcessEnvironment.set(FakeProcessEnvironment.INSTANCE, true);

        // clear out any old performance tracker and update graph
        UpdatePerformanceTracker.resetForUnitTests();
        PeriodicUpdateGraph.removeInstance(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME);
        // the primary PUG needs to be named DEFAULT or else UpdatePerformanceTracker will fail to initialize
        PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();

        // initialize the unit test's execution context
        executionContext = TestExecutionContext.createForUnitTests().open();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();
        oldMemoize = QueryTable.setMemoizeResults(false);
        oldReporter = AsyncClientErrorNotifier.setReporter(this);
        errors = null;
        livenessScopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);

        oldLogEnabled = QueryCompilerImpl.setLogEnabled(ENABLE_QUERY_COMPILER_LOGGING);
        oldSerialSafe = updateGraph.setSerialTableOperationsSafe(true);
        AsyncErrorLogger.init();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    public void tearDown() throws Exception {
        // shutdown the UPT before we check for leaked chunks
        UpdatePerformanceTracker.resetForUnitTests();
        livenessScopeCloseable.close();

        ChunkPoolReleaseTracking.checkAndDisable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.setSerialTableOperationsSafe(oldSerialSafe);
        QueryCompilerImpl.setLogEnabled(oldLogEnabled);

        // reset the execution context
        executionContext.close();

        AsyncClientErrorNotifier.setReporter(oldReporter);
        QueryTable.setMemoizeResults(oldMemoize);
        updateGraph.resetForUnitTests(true);

        if (oldProcessEnvironment == null) {
            ProcessEnvironment.clear();
        } else {
            ProcessEnvironment.set(oldProcessEnvironment, true);
        }

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

    public class ExpectingError implements SafeCloseable {
        final boolean originalExpectError = getExpectError();

        public ExpectingError() {
            setExpectError(true);
        }

        @Override
        public void close() {
            setExpectError(originalExpectError);
        }
    }

    public <T> T allowingError(Supplier<T> function, Predicate<List<Throwable>> errorsAcceptable) {
        final boolean original = getExpectError();
        T retval;
        try {
            setExpectError(true);
            retval = function.get();
        } finally {
            setExpectError(original);
        }
        if (errors != null && !errorsAcceptable.test(errors)) {
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

    public static void simulateShiftAwareStep(int targetUpdateSize, Random random, QueryTable table,
            ColumnInfo[] columnInfo, EvalNuggetInterface[] en) {
        simulateShiftAwareStep("", targetUpdateSize, random, table, columnInfo, en);
    }

    public static void simulateShiftAwareStep(final String ctxt, int targetUpdateSize, Random random, QueryTable table,
            ColumnInfo[] columnInfo, EvalNuggetInterface[] en) {
        simulateShiftAwareStep(GenerateTableUpdates.DEFAULT_PROFILE, ctxt, targetUpdateSize, random, table, columnInfo,
                en);
    }

    public static void simulateShiftAwareStep(final GenerateTableUpdates.SimulationProfile simulationProfile,
            final String ctxt, int targetUpdateSize, Random random, QueryTable table, ColumnInfo[] columnInfo,
            EvalNuggetInterface[] en) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates
                .generateShiftAwareTableUpdates(simulationProfile, targetUpdateSize, random, table, columnInfo));
        TstUtils.validate(ctxt, en);
        // The EvalNugget test cases end up generating very big listener DAGs, for at each step we create a brand new
        // live incarnation of the table. This can make debugging a bit awkward, so sometimes it is convenient to
        // prune the tree after each validation. The reason not to do it, however, is that this will sometimes expose
        // bugs with shared indices getting updated.
        // System.gc();
    }

    public class ErrorExpectation implements SafeCloseable {
        final boolean originalExpectError;

        public ErrorExpectation() {
            originalExpectError = expectError;
            expectError = true;
        }

        @Override
        public void close() {
            expectError = originalExpectError;
        }
    }
}
