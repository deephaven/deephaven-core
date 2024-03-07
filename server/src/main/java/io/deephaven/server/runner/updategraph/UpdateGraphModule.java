//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner.updategraph;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;

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
    public static UpdateGraph provideUpdateGraph(
            final ThreadInitializationFactory threadInitializationFactory,
            final OperationInitializer operationInitializer) {
        return PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .threadInitializationFactory(threadInitializationFactory)
                .operationInitializer(operationInitializer)
                .existingOrBuild();
    }

    @Provides
    @Singleton
    public static OperationInitializer provideOperationInitializer(
            final ThreadInitializationFactory factory) {
        return new OperationInitializationThreadPool(factory);
    }
}
