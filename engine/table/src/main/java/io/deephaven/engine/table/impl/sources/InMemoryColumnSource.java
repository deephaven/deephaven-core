package io.deephaven.engine.table.impl.sources;

/**
 * This is a marker interface for a column source that is entirely within memory; therefore select operations should not
 * try to copy it into memory a second time.
 */
public interface InMemoryColumnSource {
}
