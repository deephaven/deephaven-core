package io.deephaven.api.agg.key;

public abstract class KeyEmptyBase extends KeyBase {

    @Override
    public final int hashCode() {
        // Immutables uses a hashCode of 0 for empty objects.
        // Would be better to give them "proper" hashCodes so they don't always collide in map.
        // https://github.com/immutables/immutables/issues/1349
        return getClass().hashCode();
    }
}
