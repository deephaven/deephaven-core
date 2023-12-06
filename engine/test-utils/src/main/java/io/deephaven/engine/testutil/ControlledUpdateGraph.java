package io.deephaven.engine.testutil;

import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;

// TODO (deephaven-core#3886): Extract test functionality from PeriodicUpdateGraph
public class ControlledUpdateGraph extends PeriodicUpdateGraph {
    public ControlledUpdateGraph(String name, boolean allowUnitTestMode, long targetCycleDurationMillis,
            long minimumCycleDurationToLogNanos, int numUpdateThreads,
            ThreadInitializationFactory threadInitializationFactory) {
        super(name, allowUnitTestMode, targetCycleDurationMillis, minimumCycleDurationToLogNanos, numUpdateThreads,
                threadInitializationFactory);
    }
}
