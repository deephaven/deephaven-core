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
        final UpdateGraph updateGraph = PeriodicUpdateGraph.newBuilder("TEST")
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();
        return TestExecutionContext.createForUnitTests().withUpdateGraph(updateGraph);
    }
}
