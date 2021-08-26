package io.deephaven.db.tables.live;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.v2.utils.AbstractNotification;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * One-shot {@link NotificationQueue.Notification} that can be delivered when a set of
 * {@link NotificationQueue.Dependency dependencies} are satisfied. This allows for an external observer to wait for
 * multiple dependencies to be satisfied using {@link #waitForSatisfaction(long, NotificationQueue.Dependency...)}.
 */
public final class WaitNotification extends AbstractNotification {

    private final NotificationQueue.Dependency[] dependencies;

    private int satisfiedCount;
    private volatile boolean fired;

    private WaitNotification(@NotNull final NotificationQueue.Dependency... dependencies) {
        super(false);
        this.dependencies = dependencies;
    }

    @Override
    public boolean canExecute(final long step) {
        for (; satisfiedCount < dependencies.length; ++satisfiedCount) {
            if (!dependencies[satisfiedCount].satisfied(step)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(getClass().getSimpleName()).append(": for dependencies")
                .append(LogOutput.APPENDABLE_COLLECTION_FORMATTER, Arrays.asList(dependencies));
    }

    @Override
    public void run() {
        fired = true;
        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * Wait for this notification to fire.
     *
     * @throws InterruptedException If this thread was interrupted while waiting
     */
    private void await() throws InterruptedException {
        while (!fired) {
            synchronized (this) {
                if (!fired) {
                    wait();
                }
            }
        }
    }

    /**
     * Wait for all of the specified dependencies to be satisfied on the specified step.
     *
     * @param step The step to wait for satisfaction on
     * @param dependencies The dependencies to wait for
     * @return True if the dependencies became satisfied on the specified step, false if the cycle had already completed
     */
    public static boolean waitForSatisfaction(final long step,
            @NotNull final NotificationQueue.Dependency... dependencies) {
        final WaitNotification waitNotification = new WaitNotification(dependencies);
        if (LiveTableMonitor.DEFAULT.maybeAddNotification(waitNotification, step)) {
            try {
                waitNotification.await();
            } catch (InterruptedException e) {
                throw new QueryCancellationException("Interrupted while awaiting dependency satisfaction for "
                        + Arrays.stream(dependencies).map(Objects::toString).collect(Collectors.joining(","))
                        + " on step " + step, e);
            }
            return true;
        }
        return false;
    }
}
