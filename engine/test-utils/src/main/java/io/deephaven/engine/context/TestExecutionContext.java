package io.deephaven.engine.context;

import io.deephaven.auth.AuthContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.updategraph.UpdateGraph;

public class TestExecutionContext {
    public static ExecutionContext createForUnitTests() {
        UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        if (!(updateGraph instanceof ControlledUpdateGraph)) {
            updateGraph = new ControlledUpdateGraph();
            // noinspection resource
            ExecutionContext.getContext().withUpdateGraph(updateGraph).open();
        }
        return new ExecutionContext.Builder(new AuthContext.SuperUser())
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompiler.createForUnitTests())
                .setUpdateGraph(updateGraph)
                .build();
    }
}
