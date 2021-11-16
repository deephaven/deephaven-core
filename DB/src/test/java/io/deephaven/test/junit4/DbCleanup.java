package io.deephaven.test.junit4;

import io.deephaven.db.v2.LiveTableTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * When you want to extend LiveTableTestCase, but you need to use JUnit 4 annotations, like @Category
 * or @RunWith(Suite.class), then instead of extending LiveTableTestCase, you should instead create a `@Rule public
 * final DbCleanup field = new DbCleanup();`.
 */
public class DbCleanup extends LiveTableTestCase implements TestRule {
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    // We use this class as a field in JUnit 4 tests which should not extend TestCase. This method is a no-op test
    // method so when we are detected as a JUnit3 test, we do not fail
    public void testMethodSoThisIsValidJUnit3() {}

    public static boolean printTableUpdates() {
        return LiveTableTestCase.printTableUpdates;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                setUp();
                try {
                    statement.evaluate();
                } finally {
                    tearDown();
                }
            }
        };
    }
}
