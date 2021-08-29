package io.deephaven.engine.v2;

/**
 * An object that can report if it is systemically important.
 */
public interface SystemicObject {
    /**
     * Returns true if this is a systemically important object (see {@link io.deephaven.engine.tables.utils.SystemicObjectTracker}).
     *
     * @return true if this is a systemically important object, false otherwise.
     */
    default boolean isSystemicObject() {
        return true;
    }

    /**
     * Mark this object as systemically important.
     */
    default void markSystemic() {}
}
