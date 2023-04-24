/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;


import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.updategraph.UpdateContext;

import java.io.Serializable;

public abstract class WhereFilterLivenessArtifactImpl extends LivenessArtifact implements WhereFilter, Serializable {

    protected final UpdateContext updateContext;

    private boolean isAutomatedFilter = false;

    public WhereFilterLivenessArtifactImpl() {
        updateContext = UpdateContext.get();
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
