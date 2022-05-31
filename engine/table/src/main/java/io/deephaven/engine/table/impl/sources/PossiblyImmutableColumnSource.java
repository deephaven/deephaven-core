package io.deephaven.engine.table.impl.sources;

/**
 * An interface for ColumnSources that can be marked immutable by the creator rather than as a fundamental property of
 * their implementation.
 */
public interface PossiblyImmutableColumnSource {
    /**
     * Set this column source as having an immutable result.
     */
    void setImmutable();
}
