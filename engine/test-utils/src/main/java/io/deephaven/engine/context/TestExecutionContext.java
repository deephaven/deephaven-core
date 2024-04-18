//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.table.impl.OperationInitializationThreadPool;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.thread.ThreadInitializationFactory;

public class TestExecutionContext {

    public static final OperationInitializationThreadPool OPERATION_INITIALIZATION =
            new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP);

    public static final ControlledUpdateGraph UPDATE_GRAPH = new ControlledUpdateGraph(OPERATION_INITIALIZATION);

    public static ExecutionContext createForUnitTests() {
        return new ExecutionContext.Builder(new AuthContext.SuperUser())
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompilerImpl.createForUnitTests())
                .setUpdateGraph(UPDATE_GRAPH)
                .setOperationInitializer(OPERATION_INITIALIZATION)
                .build();
    }
}
