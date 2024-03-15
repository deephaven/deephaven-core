//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;

import javax.inject.Singleton;

@Module
public class ExecutionContextUnitTestModule {
    @Provides
    @Singleton
    public ExecutionContext provideExecutionContext() {
        // the primary PUG needs to be named DEFAULT or else UpdatePerformanceTracker will fail to initialize
        final UpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();
        return TestExecutionContext.createForUnitTests().withUpdateGraph(updateGraph);
    }
}
