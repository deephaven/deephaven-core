package io.deephaven.db.v2.sources.chunk;

/**
 * Resettable {@link Context} interface, for contexts that must be reset between steps of an
 * operation (e.g. when advancing to a new region, or a new chunk of ordered keys).
 */
public interface ResettableContext extends Context {

    /**
     * Reset this context before it will be used again.
     */
    void reset();
}
