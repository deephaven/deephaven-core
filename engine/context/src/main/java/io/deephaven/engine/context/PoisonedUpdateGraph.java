package io.deephaven.engine.context;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;
import io.deephaven.util.locks.AwareFunctionalLock;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class PoisonedUpdateGraph implements UpdateGraph {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedUpdateGraph.class);
    public static final PoisonedUpdateGraph INSTANCE = new PoisonedUpdateGraph();

    private PoisonedUpdateGraph() {}

    private <T> T fail() {
        logger.error().append("No ExecutionContext provided, cannot use QueryLibrary. If this is being run in a ")
                .append("thread, did you specify an ExecutionContext for the thread? Please refer to the ")
                .append("documentation on ExecutionContext for details.").endl();
        throw new NoExecutionContextRegisteredException();
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
        return fail();
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
    public void requestRefresh() {
        fail();
    }
}
