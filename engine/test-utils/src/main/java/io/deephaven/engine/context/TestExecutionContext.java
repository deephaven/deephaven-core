package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.testutil.ControlledUpdateGraph;

public class TestExecutionContext {

    public static ExecutionContext createForUnitTests() {
        return new ExecutionContext.Builder(new AuthContext.SuperUser())
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompiler.createForUnitTests())
                .setUpdateGraph(ControlledUpdateGraph.INSTANCE)
                .setOperationInitializer(new OperationInitializationThreadPool(i -> i))
                .build();
    }
}
