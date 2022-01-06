package io.deephaven.plugin.type;

/**
 * The plugin interface for registration of {@link ObjectType object types}.
 */
public interface ObjectTypePlugin {

    /**
     * The registration entrypoint for {@link ObjectType object types}.
     *
     * @param callback the callback
     */
    void registerInto(ObjectTypeCallback callback);
}
