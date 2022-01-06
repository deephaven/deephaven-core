package io.deephaven.plugin.type;

/**
 * The registration interface for {@link ObjectType}.
 */
public interface ObjectTypeCallback {

    /**
     * Registers {@code objectType}.
     *
     * @param objectType the object type
     */
    void registerObjectType(ObjectType objectType);
}
