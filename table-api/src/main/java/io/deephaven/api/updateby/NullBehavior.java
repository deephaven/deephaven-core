package io.deephaven.api.updateby;

/**
 * The action that should be taken when the previous value is {@code null}
 */
public enum NullBehavior {
    /**
     * In the case of Current - null, the null dominates so Column[i] - null = null
     */
    NullDominates,

    /**
     * In the case of Current - null, the current value dominates so Column[i] - null = Column[i]
     */
    ValueDominates
}
