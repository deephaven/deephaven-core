//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot;

import io.deephaven.engine.liveness.DelegatingLivenessNode;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.util.FigureWidgetMarker;
import io.deephaven.engine.util.LiveWidget;
import io.deephaven.engine.util.LiveWidgetVisibilityProvider;
import io.deephaven.plot.util.tables.PartitionedTableHandle;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.util.annotations.ScriptApi;

import java.util.*;

/**
 * Displayable version of a Figure.
 */
public class FigureWidget extends FigureImpl
        implements LiveWidget, LiveWidgetVisibilityProvider, FigureWidgetMarker, DelegatingLivenessNode {

    private static final long serialVersionUID = 763409998768966385L;
    private String[] validGroups;

    /**
     * By making this a non-static inner class, we can do two things:
     * <ul>
     * <li>we can call the protected constructors</li>
     * <li>any hard reference made to the liveness artifact itself will ensure reachability to the FigureWidget (this is
     * uncommon, but technically supported)</li>
     * </ul>
     */
    private final LivenessArtifact livenessImpl = new LivenessArtifact() {};

    public FigureWidget(final FigureImpl figure) {
        super(figure);
        getFigure().consolidatePartitionedTables();

        getFigure().getTableHandles().stream()
                .map(TableHandle::getTable)
                .filter(DynamicNode::notDynamicOrIsRefreshing)
                .forEach(this::manage);
        getFigure().getPartitionedTableHandles().stream()
                .map(PartitionedTableHandle::getPartitionedTable)
                .filter(DynamicNode::notDynamicOrIsRefreshing)
                .forEach(this::manage);
    }

    @Override
    public LivenessNode asLivenessNode() {
        return livenessImpl;
    }

    @ScriptApi
    @Override
    public String[] getValidGroups() {
        return validGroups;
    }

    @ScriptApi
    public void setValidGroups(final String... validGroups) {
        this.validGroups = validGroups;
    }

    @ScriptApi
    public void setValidGroups(final Collection<String> validGroups) {
        setValidGroups(validGroups.toArray(String[]::new));
    }
}
