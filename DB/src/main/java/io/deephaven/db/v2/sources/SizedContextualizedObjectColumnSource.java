package io.deephaven.db.v2.sources;

/**
 * Interface for {@link ColumnSource} implementations that are both {@link SizedColumnSource}s and
 * {@link ContextualizedObjectColumnSource}s.
 */
public interface SizedContextualizedObjectColumnSource<DATA_TYPE>
        extends SizedColumnSource<DATA_TYPE>, ContextualizedObjectColumnSource<DATA_TYPE> {
}
