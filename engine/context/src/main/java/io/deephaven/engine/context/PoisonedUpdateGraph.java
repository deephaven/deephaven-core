package io.deephaven.engine.context;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.io.log.LogEntry;
import io.deephaven.util.ExecutionContextRegistrationException;
import io.deephaven.util.locks.AwareFunctionalLock;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class PoisonedUpdateGraph implements UpdateGraph {

    public static final PoisonedUpdateGraph INSTANCE = new PoisonedUpdateGraph();

    // this frozen clock is always Idle
    private final LogicalClock frozenClock = () -> 1;

    private PoisonedUpdateGraph() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("UpdateGraph");
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("PoisonedUpdateGraph");
    }

    @Override
    public boolean satisfied(long step) {
        return fail();
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return this;
    }

    @Override
    public String getName() {
        return "PoisonedUpdateGraph";
    }

    @Override
    public void addNotification(@NotNull Notification notification) {
        fail();
    }

    @Override
    public void addNotifications(@NotNull Collection<? extends Notification> notifications) {
        fail();
    }

    @Override
    public boolean maybeAddNotification(@NotNull Notification notification, long deliveryStep) {
        return fail();
    }

    @Override
    public AwareFunctionalLock sharedLock() {
        return fail();
    }

    @Override
    public AwareFunctionalLock exclusiveLock() {
        return fail();
    }

    @Override
    public LogicalClock clock() {
        return frozenClock;
    }

    @Override
    public int parallelismFactor() {
        return fail();
    }

    @Override
    public LogEntry logDependencies() {
        return fail();
    }

    @Override
    public boolean currentThreadProcessesUpdates() {
        return fail();
    }

    @Override
    public boolean serialTableOperationsSafe() {
        return fail();
    }

    @Override
    public boolean setSerialTableOperationsSafe(boolean newValue) {
        return fail();
    }

    @Override
    public void addSource(@NotNull Runnable updateSource) {
        fail();
    }

    @Override
    public void removeSource(@NotNull Runnable updateSource) {
        fail();
    }

    @Override
    public boolean supportsRefreshing() {
        return false;
    }

    @Override
    public void requestRefresh() {
        fail();
    }
}
