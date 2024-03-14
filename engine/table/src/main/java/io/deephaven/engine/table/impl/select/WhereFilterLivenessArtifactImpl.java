//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;


import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.liveness.LivenessArtifact;

import java.io.Serializable;

public abstract class WhereFilterLivenessArtifactImpl extends LivenessArtifact implements WhereFilter, Serializable {

    protected final UpdateGraph updateGraph;

    private boolean isAutomatedFilter = false;

    public WhereFilterLivenessArtifactImpl() {
        updateGraph = ExecutionContext.getContext().getUpdateGraph();
    }

    @Override
    public boolean isAutomatedFilter() {
        return isAutomatedFilter;
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        isAutomatedFilter = value;
    }
}
