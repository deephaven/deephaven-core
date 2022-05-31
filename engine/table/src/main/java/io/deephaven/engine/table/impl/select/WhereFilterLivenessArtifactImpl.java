package io.deephaven.engine.table.impl.select;


import io.deephaven.engine.liveness.LivenessArtifact;

import java.io.Serializable;

public abstract class WhereFilterLivenessArtifactImpl extends LivenessArtifact implements WhereFilter, Serializable {
    private boolean isAutomatedFilter = false;

    @Override
    public boolean isAutomatedFilter() {
        return isAutomatedFilter;
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        isAutomatedFilter = value;
    }
}
