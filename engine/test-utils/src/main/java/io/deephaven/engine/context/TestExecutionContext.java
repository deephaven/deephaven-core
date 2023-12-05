package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;

public class TestExecutionContext {
    private static final PeriodicUpdateGraph UPDATE_GRAPH =
            new PeriodicUpdateGraph("TEST", true, 1000, 25, -1, ThreadInitializationFactory.NO_OP);
    private static final OperationInitializationThreadPool OPERATION_INITIALIZATION =
            new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP);

    public static ExecutionContext createForUnitTests() {
        return new ExecutionContext.Builder(new AuthContext.SuperUser())
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompiler.createForUnitTests())
                .setUpdateGraph(UPDATE_GRAPH)
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .build();
    }
}
