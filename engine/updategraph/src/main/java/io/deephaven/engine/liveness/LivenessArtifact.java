/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

/**
 * A query engine artifact that is also a {@link LivenessNode}. These referents are added to the current top of the
 * {@link LivenessScopeStack} on construction or deserialization.
 */
public class LivenessArtifact extends ReferenceCountedLivenessNode implements Serializable {

    private static final long serialVersionUID = 1L;

    protected LivenessArtifact() {
        this(false);
    }

    /**
     * @param enforceStrongReachability Whether this {@link LivenessArtifact} should maintain strong references to its
     *        referents
     */
    protected LivenessArtifact(final boolean enforceStrongReachability) {
        super(enforceStrongReachability);
        manageWithCurrentScope();
    }

    /**
     * Ensure that transient fields are initialized properly on deserialization.
     *
     * @param objectInputStream The object input stream
     */
    private void readObject(@NotNull final ObjectInputStream objectInputStream)
            throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
        initializeTransientFieldsForLiveness();
        manageWithCurrentScope();
    }

    /**
     * Add this artifact to the current manager provided by the {@link LivenessScopeStack}.
     */
    public final void manageWithCurrentScope() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        LivenessScopeStack.peek().manage(this);
    }
}
