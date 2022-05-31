package io.deephaven.plugin.type;

/**
 * The {@link ObjectType} specific registration.
 */
public interface ObjectTypeRegistration {

    /**
     * Register {@code objectType}.
     *
     * @param objectType the object type
     */
    void register(ObjectType objectType);
}
