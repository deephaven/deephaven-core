//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

/**
 * A sparse in-memory column source of Booleans, which stores each value as a byte, as
 * {@link io.deephaven.util.BooleanUtils#booleanAsByte(Boolean)} encodes it, in a {@link ByteAdaptiveSparseArraySource}.
 * It may be reinterpreted as that byte source.
 */
public class BooleanAdaptiveSparseArraySource extends WritableByteAsBooleanColumnSource
        implements InMemoryColumnSource, PossiblyImmutableColumnSource, RowSetShiftCallback, SparseBlockColumnSource,
        WritableSourceWithPrepareForParallelPopulation {

    private final ByteAdaptiveSparseArraySource byteSource;

    public BooleanAdaptiveSparseArraySource() {
        this(new ByteAdaptiveSparseArraySource());
    }

    private BooleanAdaptiveSparseArraySource(@NotNull final ByteAdaptiveSparseArraySource byteSource) {
        super(byteSource);
        this.byteSource = byteSource;
    }

    @Override
    public void setNull(@NotNull final RowSequence rowSequence) {
        byteSource.setNull(rowSequence);
    }

    @Override
    public void startTrackingPrevValues() {
        byteSource.startTrackingPrevValues();
    }

    @Override
    public void setImmutable() {
        byteSource.setImmutable();
    }

    @Override
    public void shift(final RowSet rowSet, final long shiftDelta) {
        byteSource.shift(rowSet, shiftDelta);
    }

    @Override
    public void clearBlocks(final RowSet blocksToClear, final RowSet removeBlocks2, final RowSet removeBlocks1,
            final boolean empty) {
        byteSource.clearBlocks(blocksToClear, removeBlocks2, removeBlocks1, empty);
    }

    @TestUseOnly
    @Override
    public long estimateSize() {
        return byteSource.estimateSize();
    }

    @Override
    public int parallelPopulationAlignment() {
        return byteSource.parallelPopulationAlignment();
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence) {
        byteSource.prepareForParallelPopulation(rowSequence);
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence rowSequence, final boolean alignedDivisions) {
        byteSource.prepareForParallelPopulation(rowSequence, alignedDivisions);
    }
}
