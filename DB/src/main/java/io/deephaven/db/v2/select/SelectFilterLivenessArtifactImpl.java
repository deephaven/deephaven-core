package io.deephaven.db.v2.select;


import io.deephaven.db.util.liveness.LivenessArtifact;

import java.io.Serializable;

public abstract class SelectFilterLivenessArtifactImpl extends LivenessArtifact implements SelectFilter, Serializable {
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
