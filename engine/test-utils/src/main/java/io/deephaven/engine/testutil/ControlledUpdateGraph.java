//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil;

import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;

// TODO (deephaven-core#3886): Extract test functionality from PeriodicUpdateGraph
public class ControlledUpdateGraph extends PeriodicUpdateGraph {
    public ControlledUpdateGraph(final OperationInitializer operationInitializer) {
        super("TEST", true, 1000, 25, -1, ThreadInitializationFactory.NO_OP, operationInitializer);
    }
}
