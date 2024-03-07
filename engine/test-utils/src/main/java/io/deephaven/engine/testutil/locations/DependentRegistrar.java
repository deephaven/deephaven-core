//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class DependentRegistrar implements UpdateSourceRegistrar, Runnable {

    private final NotificationQueue.Dependency[] dependencies;
    private final UpdateGraph updateGraph;

    private final List<Runnable> dependentSources = new ArrayList<>();

    public DependentRegistrar(@NotNull final NotificationQueue.Dependency... dependencies) {
        this.dependencies = dependencies;
        updateGraph = ExecutionContext.getContext().getUpdateGraph();
        updateGraph.addSource(this);
    }

    @Override
    public synchronized void addSource(@NotNull final Runnable updateSource) {
        dependentSources.add(updateSource);
    }

    @Override
    public synchronized void removeSource(@NotNull final Runnable updateSource) {
        dependentSources.remove(updateSource);
    }

    @Override
    public void requestRefresh() {
        updateGraph.requestRefresh();
    }

    @Override
    public void run() {
        updateGraph.addNotification(new AbstractNotification(false) {
            @Override
            public boolean canExecute(final long step) {
                return DependentRegistrar.this.satisfied(step);
            }

            @Override
            public void run() {
                synchronized (DependentRegistrar.this) {
                    final int sourcesSize = dependentSources.size();
                    /*
                     * We're simulating a scenario wherein TableLocation.Listeners add and remove locations
                     * asynchronously w.r.t. the update graph cycle. For our actual results (Regioned or Partitioned) to
                     * match our expected value on a given cycle, our "pushes" must be completed before the dependent's
                     * location processing event runs. As a result, we need to run the dependentSources in reverse order
                     * to ensure that the first source always runs after all the others, so it can successfully poll
                     * everything that should have been pushed for this cycle.
                     */
                    for (int si = sourcesSize - 1; si >= 0; --si) {
                        dependentSources.get(si).run();
                    }
                }
            }
        });
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("DependentRegistrar[")
                .append(LogOutput.APPENDABLE_ARRAY_FORMATTER, dependencies).append(']');
    }

    @Override
    public boolean satisfied(final long step) {
        return updateGraph.satisfied(step)
                && Arrays.stream(dependencies).allMatch(dependency -> dependency.satisfied(step));
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }
}
