package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.BasicTableDataListener;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.util.datastructures.SubscriptionSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implement optional subscription support suitable for multiple TableDataService components.
 *
 * @param <LISTENER_TYPE> A bound on the type of listener supported by this aggregator's subscriptions
 */
public abstract class SubscriptionAggregator<LISTENER_TYPE extends BasicTableDataListener> {

    protected final SubscriptionSet<LISTENER_TYPE> subscriptions;

    private enum ActivationState {
        EMPTY, PENDING, ACTIVE, FAILED
    }

    private ActivationState activationState = ActivationState.EMPTY;

    SubscriptionAggregator(final boolean supportsSubscriptions) {
        subscriptions = supportsSubscriptions ? new SubscriptionSet<>() : null;
    }

    @SuppressWarnings("WeakerAccess") // This must be public, it's being used for "duck typing"
    public final boolean supportsSubscriptions() {
        return subscriptions != null;
    }

    public final void subscribe(@NotNull final LISTENER_TYPE listener) {
        if (!supportsSubscriptions()) {
            throw new UnsupportedOperationException(this + " doesn't support subscriptions");
        }
        final SubscriptionSet<LISTENER_TYPE>.Entry entry = subscriptions.makeEntryFor(listener);
        synchronized (subscriptions) {
            if (subscriptions.add(listener, entry)) {
                activationState = ActivationState.PENDING;
                activateUnderlyingDataSource();
            }
            while (activationState == ActivationState.PENDING) {
                try {
                    subscriptions.wait();
                } catch (InterruptedException e) {
                    // noinspection finally
                    try {
                        unsubscribe(listener);
                    } finally {
                        // noinspection ThrowFromFinallyBlock
                        throw new TableDataException("Exception while subscribing to " + this, e);
                    }
                }
            }
            if (activationState == ActivationState.ACTIVE) {
                entry.activate();
                deliverInitialSnapshot(listener);
            }
        }
    }

    /**
     * Prompt listeners to record current state, under the subscriptions lock.
     *
     * @param listener The listener to notify
     */
    protected abstract void deliverInitialSnapshot(@NotNull final LISTENER_TYPE listener);

    private void onActivationDone(final ActivationState result) {
        activationState = result;
        subscriptions.notifyAll();
    }

    /**
     * Method to override in order to observe successful activation.
     */
    protected void postActivationHook() {}

    final void onEmpty() {
        onActivationDone(ActivationState.EMPTY);
        deactivateUnderlyingDataSource();
    }

    public final void unsubscribe(@NotNull final LISTENER_TYPE listener) {
        if (!supportsSubscriptions()) {
            throw new UnsupportedOperationException(this + " doesn't support subscriptions");
        }
        synchronized (subscriptions) {
            if (subscriptions.remove(listener)) {
                onEmpty();
            }
        }
    }

    /**
     * Check if this subscription aggregator still has any valid subscribers - useful if there may have been no
     * notifications delivered for some time, as a test to determine whether work should be done to maintain the
     * underlying subscription.
     *
     * @return true if there are valid subscribers, else false
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean checkHasSubscribers() {
        synchronized (subscriptions) {
            if (subscriptions.collect()) {
                onEmpty();
                return false;
            }
            return true;
        }
    }

    /**
     * <p>
     * Refresh and activate update pushing from the implementing class.
     * <p>
     * If the implementation will deliver notifications in a different thread than the one that calls this method, then
     * this method must be asynchronous - that is, it must not block pending delivery of results. <em>This requirement
     * holds even if that other thread has nothing to do with the initial activation request!</em>
     *
     * <p>
     * Listeners should guard against duplicate notifications, especially if the implementation delivers synchronous
     * notifications.
     * <p>
     * The implementation should call activationSuccessful() when done activating and delivering initial run results,
     * unless activationFailed() was called instead.
     * <p>
     * Must be called under the subscription lock.
     */
    protected void activateUnderlyingDataSource() {
        throw new UnsupportedOperationException();
    }

    /**
     * Notify the implementation that activation has completed. This may be invoked upon "re-activation" of an existing
     * subscription, in which case it is effectively a no-op. This is public because it is called externally by services
     * implementing subscriptions.
     *
     * @param token A subscription-related object that the subclass can use to match a notification
     */
    public final <T> void activationSuccessful(@Nullable final T token) {
        if (!supportsSubscriptions()) {
            throw new IllegalStateException(
                    this + ": completed activations are unexpected when subscriptions aren't supported");
        }
        synchronized (subscriptions) {
            if (!matchSubscriptionToken(token)) {
                return;
            }
            if (activationState == ActivationState.PENDING) {
                onActivationDone(ActivationState.ACTIVE);
            }
        }
        postActivationHook();
    }

    /**
     * Deliver an exception triggered while activating or maintaining the underlying data source. The underlying data
     * source is implicitly deactivated. This is public because it is called externally by services implementing
     * subscriptions.
     *
     * @param token A subscription-related object that the subclass can use to match a notification
     * @param exception The exception
     */
    public final <T> void activationFailed(@Nullable final T token, @NotNull final TableDataException exception) {
        if (!supportsSubscriptions()) {
            throw new IllegalStateException(
                    this + ": asynchronous exceptions are unexpected when subscriptions aren't supported", exception);
        }
        synchronized (subscriptions) {
            if (!matchSubscriptionToken(token)) {
                return;
            }
            if (activationState == ActivationState.PENDING) {
                onActivationDone(ActivationState.FAILED); // NB: This can be done before or after the notification
                                                          // delivery, since we're holding the lock.
            }
            subscriptions.deliverNotification(BasicTableDataListener::handleException, exception, false);
            if (!subscriptions.isEmpty()) {
                subscriptions.clear();
            }
        }
    }

    /**
     * Deactivate pushed updates from the implementing class. Must be called under the subscription lock.
     */
    protected void deactivateUnderlyingDataSource() {
        throw new UnsupportedOperationException();
    }

    /**
     * Verify that a notification pertains to a currently-active subscription. Must be called under the subscription
     * lock.
     *
     * @param token A subscription-related object that the subclass can use to match a notification
     * @return True iff notification delivery should proceed
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected <T> boolean matchSubscriptionToken(final T token) {
        throw new UnsupportedOperationException();
    }
}
