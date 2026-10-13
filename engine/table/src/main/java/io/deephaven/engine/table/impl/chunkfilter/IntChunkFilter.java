//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkFilter and run "./gradlew replicateChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfilter;

/**
 * A {@link ChunkFilter} for int values that tests each value with {@link #matches(int)}.
 * <p>
 * This class deliberately does not implement the {@code filter} and {@code filterAnd} loops. Loops shared by every
 * subclass would see many receivers at the {@code matches} call once a few filter types have run through them, and the
 * JIT would leave it as a virtual call per value. Each subclass instead carries its own identical copy of the loops, so
 * that its {@code matches} call has only one receiver and can be inlined.
 */
public abstract class IntChunkFilter implements ChunkFilter {
    public abstract boolean matches(int value);
}
