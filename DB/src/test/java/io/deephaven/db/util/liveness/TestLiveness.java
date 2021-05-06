package io.deephaven.db.util.liveness;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.TstUtils;
import junit.framework.TestCase;
import org.junit.Test;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Unit tests for liveness code.
 */
public class TestLiveness extends TestCase {

    private boolean oldCheckLtm;
    private LivenessScope scope;

    @Setup
    public void setUp() throws Exception {
        super.setUp();
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests();
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
    }

    @TearDown
    public void tearDown() throws Exception {
        super.tearDown();
        LivenessScopeStack.pop(scope);
        scope.release();
        LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
        LiveTableMonitor.DEFAULT.resetForUnitTests();
    }

    @SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
    @Test
    public void testRecursion() {
        //noinspection AutoBoxing
        final Table input = TstUtils.testRefreshingTable(
                TstUtils.i(2, 3, 6, 7, 8, 10, 12, 15, 16),
                TstUtils.c("GroupedInts", 1, 1, 2, 2, 2, 3, 3, 3, 3)
        );
        Table result = null;
        for (int ii = 0; ii < 4096; ++ii) {
            if (result == null) {
                result = input;
            } else {
                result = TableTools.merge(result, input).updateView("GroupedInts=GroupedInts+1").updateView("GroupedInts=GroupedInts-1");
            }
        }
    }
}
