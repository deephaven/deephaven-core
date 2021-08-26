package io.deephaven.db.util.liveness;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;

/**
 * A query engine artifact that is also a {@link LivenessNode}. These referents are added to the
 * current top of the {@link LivenessScopeStack} on construction or deserialization.
 */
public class LivenessArtifact extends ReferenceCountedLivenessNode implements Serializable {

    private static final long serialVersionUID = 1L;

    protected LivenessArtifact() {
        super(false);
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

    /**
     * <p>
     * If this manages referent one or more times, drop one such reference.
     *
     * @param referent The referent to drop
     */
    protected final void unmanage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        tracker.dropReference(referent);
    }

    /**
     * <p>
     * If this artifact is still live and it manages referent one or more times, drop one such
     * reference.
     *
     * @param referent The referent to drop
     */
    protected final void tryUnmanage(@NotNull final LivenessReferent referent) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (tryRetainReference()) {
            try {
                tracker.dropReference(referent);
            } finally {
                dropReference();
            }
        }
    }

    /**
     * <p>
     * For each referent in referents, if this manages referent one or more times, drop one such
     * reference.
     *
     * @param referents The referents to drop
     */
    @SuppressWarnings("unused")
    protected final void unmanage(@NotNull final Collection<? extends LivenessReferent> referents) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        tracker.dropReferences(referents);
    }

    /**
     * <p>
     * For each referent in referents, if this artifact is still live and it manages referent one or
     * more times, drop one such reference.
     *
     * @param referents The referents to drop
     */
    @SuppressWarnings("unused")
    protected final void tryUnmanage(
        @NotNull final Collection<? extends LivenessReferent> referents) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (tryRetainReference()) {
            try {
                tracker.dropReferences(referents);
            } finally {
                dropReference();
            }
        }
    }
}
