/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.UpdateErrorReporter;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.db.v2.utils.AsyncClientErrorNotifier;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
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

abstract public class LiveTableTestCase extends BaseArrayTestCase implements UpdateErrorReporter {
    static public boolean printTableUpdates = Configuration.getInstance()
            .getBooleanForClassWithDefault(LiveTableTestCase.class, "printTableUpdates", false);
    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(QueryTableTestBase.class, "CompilerTools.logEnabled", false);

    private boolean oldMemoize;
    private UpdateErrorReporter oldReporter;
    private boolean expectError = false;
    private SafeCloseable scopeCloseable;
    private QueryScope originalScope;
    private boolean oldLogEnabled;
    private boolean oldCheckLtm;

    List<Throwable> errors;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();
        oldMemoize = QueryTable.setMemoizeResults(false);
        oldReporter = AsyncClientErrorNotifier.setReporter(this);
        errors = null;
        scopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);
        originalScope = QueryScope.getScope();

        oldLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    protected void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
        LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
        CompilerTools.setLogEnabled(oldLogEnabled);

        QueryScope.setScope(originalScope);
        scopeCloseable.close();
        AsyncClientErrorNotifier.setReporter(oldReporter);
        QueryTable.setMemoizeResults(oldMemoize);
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);

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
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates
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
