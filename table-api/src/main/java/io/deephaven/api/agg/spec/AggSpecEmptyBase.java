package io.deephaven.api.agg.spec;

public abstract class AggSpecEmptyBase extends AggSpecBase {

    @Override
    public final int hashCode() {
        // Immutables uses a hashCode of 0 for empty objects.
        // Would be better to give them "proper" hashCodes so they don't always collide in map.
        // https://github.com/immutables/immutables/issues/1349
        return getClass().hashCode();
    }
}
