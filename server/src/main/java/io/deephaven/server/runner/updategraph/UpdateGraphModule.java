package io.deephaven.server.runner.updategraph;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;

import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Provides a {@value PeriodicUpdateGraph#DEFAULT_UPDATE_GRAPH_NAME} named {@link UpdateGraph}.
 */
@Module
public class UpdateGraphModule {
    @Provides
    @Singleton
    @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
    public static UpdateGraph provideUpdateGraph() {
        return PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();
    }
}
