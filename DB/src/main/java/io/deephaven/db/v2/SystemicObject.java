package io.deephaven.db.v2;

/**
 * An object that can report if it is systemically important.
 */
public interface SystemicObject {
    /**
     * Returns true if this is a systemically important object (see {@link io.deephaven.db.tables.utils.SystemicObjectTracker}).
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
