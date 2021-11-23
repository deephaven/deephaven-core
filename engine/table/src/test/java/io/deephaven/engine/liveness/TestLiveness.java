package io.deephaven.engine.liveness;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.TstUtils;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for liveness code.
 */
public class TestLiveness extends TestCase {

    private boolean oldCheckUgp;
    private LivenessScope scope;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
        oldCheckUgp = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(false);
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        LivenessScopeStack.pop(scope);
        scope.release();
        UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheckUgp);
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
    @Test
    public void testRecursion() {
        // noinspection AutoBoxing
        final Table input = TstUtils.testRefreshingTable(
                TstUtils.i(2, 3, 6, 7, 8, 10, 12, 15, 16).toTracking(),
                TstUtils.c("GroupedInts", 1, 1, 2, 2, 2, 3, 3, 3, 3));
        Table result = null;
        for (int ii = 0; ii < 4096; ++ii) {
            if (result == null) {
                result = input;
            } else {
                result = TableTools.merge(result, input).updateView("GroupedInts=GroupedInts+1")
                        .updateView("GroupedInts=GroupedInts-1");
            }
        }
    }
}
