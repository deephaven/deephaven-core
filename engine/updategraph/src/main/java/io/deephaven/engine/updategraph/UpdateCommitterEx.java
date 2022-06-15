/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.function.BiConsumer;

/**
 * Tool for allowing cleanup at update cycle end as a {@link TerminalNotification}. The general pattern is to specify
 * {@code target} as <em>something</em> that contains the state to be cleaned up, and invoke an instance method on
 * {@code target} from {@code committer}. By maintaining only weak reachability to {@code target}, we avoid prolonging
 * its lifetime in order to perform cleanup tasks that would be obviated by its collection.
 * <p>
 * This variant additionally allows a {@code secondary} weakly-reachable input to be specified on a per-cycle basis,
 * allowing the UpdateCommitterEx to be stored and re-used across cycles when this pattern is necessary.
 */
public class UpdateCommitterEx<T, U> extends TerminalNotification {

    private final WeakReference<T> targetReference;
    private final BiConsumer<T, U> committer;

    private WeakReference<U> secondaryReference;
    private boolean active;

    public UpdateCommitterEx(T target, BiConsumer<T, U> committer) {
        this.targetReference = new WeakReference<>(target);
        this.committer = committer;
    }

    @Override
    public void run() {
        active = false;
        final T target = targetReference.get();
        final U secondary = secondaryReference.get();
        if (target != null && secondary != null) {
            committer.accept(target, secondary);
        }
    }

    public void maybeActivate(@NotNull final U secondary) {
        if (active) {
            return;
        }
        active = true;
        if (secondaryReference == null || secondaryReference.get() != secondary) {
            secondaryReference = new WeakReference<>(secondary);
        }
        UpdateGraphProcessor.DEFAULT.addNotification(this);
    }
}
