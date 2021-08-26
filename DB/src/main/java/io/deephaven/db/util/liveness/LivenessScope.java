package io.deephaven.db.util.liveness;

import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

/**
 * {@link LivenessNode} implementation for providing external scope to one or more {@link LivenessReferent}s.
 */
public class LivenessScope extends ReferenceCountedLivenessNode implements ReleasableLivenessManager {

    /**
     * Construct a new scope, which must be {@link #release()}d in order to release any subsequently added
     * {@link LivenessReferent}s. Will only enforce weak reachability on its {@link #manage(LivenessReferent)}ed
     * referents.
     */
    public LivenessScope() {
        this(false);
    }

    /**
     * Construct a new scope, which must be {@link #release()}d in order to release any subsequently added
     * {@link LivenessReferent}s.
     *
     * @param enforceStrongReachability Whether this {@link LivenessScope} should maintain strong references to its
     *        {@link #manage(LivenessReferent)}ed referents
     */
    public LivenessScope(boolean enforceStrongReachability) {
        super(enforceStrongReachability);
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Creating scope ").append(Utils.REFERENT_FORMATTER, this).endl();
        }
        incrementReferenceCount();
    }

    /**
     * Transfer all retained {@link LivenessReferent}s from this {@link LivenessScope} to a compatible
     * {@link LivenessManager}. Transfer support compatibility is implementation defined.
     *
     * @param other The other {@link LivenessManager}
     */
    public final void transferTo(@NotNull final LivenessManager other) {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (enforceStrongReachability) {
            throw new UnsupportedOperationException(
                    "LivenessScope does not support reference transfer if enforceStrongReachability is specified");
        }
        if (other instanceof ReferenceCountedLivenessNode) {
            tracker.transferReferencesTo(((ReferenceCountedLivenessNode) other).tracker);
        } else if (other instanceof PermanentLivenessManager) {
            tracker.makeReferencesPermanent();
        } else {
            throw new UnsupportedOperationException("Unable to transfer to unrecognized implementation class="
                    + Utils.getSimpleNameFor(other) + ", instance=" + other);
        }
    }

    /**
     * Release all referents previously added to this scope in its capacity as a {@link LivenessManager}, unless other
     * references to this scope are retained in its capacity as a {@link LivenessReferent}.
     */
    @Override
    public final void release() {
        if (Liveness.REFERENCE_TRACKING_DISABLED) {
            return;
        }
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Begin releasing scope ").append(Utils.REFERENT_FORMATTER, this)
                    .endl();
        }
        decrementReferenceCount();
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: End releasing scope ").append(Utils.REFERENT_FORMATTER, this)
                    .endl();
        }
    }
}
