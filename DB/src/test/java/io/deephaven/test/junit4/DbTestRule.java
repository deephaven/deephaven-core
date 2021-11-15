package io.deephaven.test.junit4;

import io.deephaven.db.tables.UpdateErrorReporter;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.utils.AsyncClientErrorNotifier;
import io.deephaven.util.SafeCloseable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Create and remove various aspects of the engine as part of a test. This can be used as a JUnit4 @Rule, or provides
 * static methods to be referenced from JUnit3 TestCase setup/teardown methods.
 */
public class DbTestRule implements TestRule {
    private final UpdateErrorReporter reporter;

    private boolean oldMemoize;
    private UpdateErrorReporter oldReporter;
    private QueryScope originalScope;
    private SafeCloseable scopeCloseable;

    public DbTestRule() {
        this(null);
    }

    public DbTestRule(UpdateErrorReporter reporter) {
        this.reporter = reporter;
    }

    public void setup() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();
        oldMemoize = QueryTable.setMemoizeResults(false);
        if (reporter != null) {
            oldReporter = AsyncClientErrorNotifier.setReporter(reporter);
        } else {
            oldReporter = null;
        }
        scopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);
        originalScope = QueryScope.getScope();
    }

    public void tearDown() {
        scopeCloseable.close();
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
        QueryTable.setMemoizeResults(oldMemoize);
        if (oldReporter != null) {
            AsyncClientErrorNotifier.setReporter(oldReporter);
        }
        QueryScope.setScope(originalScope);
    }

    @Override
    public Statement apply(Statement statement, Description description) {

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                setup();
                try {
                    statement.evaluate();
                } finally {
                    tearDown();
                }
            }
        };
    }


}
