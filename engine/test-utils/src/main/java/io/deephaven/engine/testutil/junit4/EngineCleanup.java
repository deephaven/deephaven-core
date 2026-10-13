//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.junit4;

import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Applies the {@link QueryTableTestBase} fixture to a test that cannot extend it - because it already has a supertype,
 * or because it wants the fixture per-rule rather than by inheritance. Declare it as
 * {@code @Rule public final EngineCleanup field = new EngineCleanup();}.
 *
 * <p>
 * Extending {@link QueryTableTestBase} directly is the simpler option and works with plain JUnit 4 annotations.
 */
public class EngineCleanup extends QueryTableTestBase implements TestRule {

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
