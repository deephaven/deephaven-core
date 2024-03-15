//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.junit4;

import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * When you want to extend RefreshingTableTestCase/QueryTableTestBase but you need to use JUnit 4 annotations, like
 * {@code @Category} or {@code @RunWith(Suite.class)}, then instead of extending RefreshingTableTestCase, you should
 * instead create a {@code @Rule public final EngineCleanup field = new EngineCleanup();}.
 */
public class EngineCleanup extends QueryTableTestBase implements TestRule {

    // We use this class as a field in JUnit 4 tests which should not extend TestCase. This method is a no-op test
    // method so when we are detected as a JUnit3 test, we do not fail
    public void testMethodSoThisIsValidJUnit3() {}

    public static boolean printTableUpdates() {
        return RefreshingTableTestCase.printTableUpdates;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                setUp();
                try (final AutoCloseable ignored = () -> tearDown()) {
                    statement.evaluate();
                }
            }
        };
    }
}
