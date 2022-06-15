/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import java.lang.ref.WeakReference;
import java.util.function.Consumer;

/**
 * Tool for allowing cleanup at update cycle end as a {@link TerminalNotification}. The general pattern is to specify
 * {@code target} as <em>something</em> that contains the state to be cleaned up, and invoke an instance method on
 * {@code target} from {@code committer}. By maintaining only weak reachability to {@code target}, we avoid prolonging
 * its lifetime in order to perform cleanup tasks that would be obviated by its collection.
 */
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
