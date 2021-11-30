/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.updategraph;

import java.lang.ref.WeakReference;
import java.util.function.Consumer;

public class UpdateCommitter<T> extends TerminalNotification {

    private final WeakReference<T> targetReference;
    private final Consumer<T> committer;
    private boolean active;

    public UpdateCommitter(T target, Consumer<T> committer) {
        this.targetReference = new WeakReference<>(target);
        this.committer = committer;
        this.active = false;
    }

    @Override
    public void run() {
        active = false;
        final T target = targetReference.get();
        if (target != null) {
            committer.accept(target);
        }
    }

    public void maybeActivate() {
        if (active) {
            return;
        }
        active = true;
        UpdateGraphProcessor.DEFAULT.addNotification(this);
    }
}
