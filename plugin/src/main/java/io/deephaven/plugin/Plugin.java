package io.deephaven.plugin;

import io.deephaven.plugin.type.ObjectType;

/**
 * A plugin is a structured extension point for user-definable behavior.
 *
 * @see ObjectType
 */
public interface Plugin extends Registration {

    /**
     * Registers {@code this} plugin into the {@code callback}.
     *
     * @param callback the callback.
     */
    @Override
    void registerInto(Callback callback);

    <T, V extends Visitor<T>> T walk(V visitor);

    interface Visitor<T> {
        T visit(ObjectType objectType);
    }
}
