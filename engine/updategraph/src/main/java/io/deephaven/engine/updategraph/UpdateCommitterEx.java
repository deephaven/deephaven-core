/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.updategraph;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.function.BiConsumer;

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
