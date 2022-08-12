/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

public abstract class IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase
        extends OperatorAggregationStateManagerOpenAddressedAlternateBase
        implements IncrementalOperatorAggregationStateManager {

    /** Our state value used when nothing is there. */
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    /**
     * The state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation).
     */
    protected ImmutableIntArraySource mainOutputPosition = new ImmutableIntArraySource();

    /**
     * The state value for the bucket, parallel to alternateKeySources (the state is an output row key for the
     * aggregation).
     */
    protected ImmutableIntArraySource alternateOutputPosition;

    /**
     * Used as a row redirection for the output key sources, updated using the mainInsertMask to identify the main vs.
     * alternate values.
     */
    protected final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    /** State variables that exist as part of the update. */
    protected MutableInt nextOutputPosition;
    protected WritableIntChunk<RowKeys> outputPositions;

    /** Output alternating column sources. */
    protected AlternatingColumnSource[] alternatingColumnSources;

    /**
     * The mask for insertion into the main table (this tells our alternating column sources which of the two sources to
     * access for a given key).
     */
    protected int mainInsertMask = 0;

    protected IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase(
            ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor);
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    protected void newAlternate() {
        alternateOutputPosition = mainOutputPosition;
        mainOutputPosition = new ImmutableIntArraySource();
        mainOutputPosition.ensureCapacity(tableSize);
        if (mainInsertMask == 0) {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(alternateKeySources[ai], mainKeySources[ai]);
                }
            }
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        } else {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(mainKeySources[ai], alternateKeySources[ai]);
                }
            }
            mainInsertMask = 0;
        }
    }

    @Override
    protected void clearAlternate() {
        super.clearAlternate();
        this.alternateOutputPosition = null;
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(
            @NotNull final SafeCloseable bc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final MutableInt nextOutputPosition,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, this::build);
        this.outputPositions = null;
        this.nextOutputPosition = null;
    }

    @Override
    public void onNextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(nextOutputPosition.intValue() + size, false);
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        Assert.eqNull(alternatingColumnSources, "alternatingColumnSources");
        alternatingColumnSources = new AlternatingColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            final Class<?> dataType = mainKeySources[kci].getType();
            final Class<?> componentType = mainKeySources[kci].getComponentType();
            if (mainInsertMask == 0) {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        mainKeySources[kci], alternateKeySources[kci]);
            } else {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        alternateKeySources[kci], mainKeySources[kci]);
            }
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot, alternatingColumnSources[kci]);
        }

        return keyHashTableSources;
    }

    @Override
    public void beginUpdateCycle() {
        // Once we're past initial state processing, we want to rehash incrementally.
        fullRehash = false;
        // At the beginning of the update cycle, we always want to do some rehash work so that we can eventually ditch
        // the alternate table.
        if (rehashPointer > 0) {
            rehashInternalPartial(CHUNK_SIZE);
        }
    }

    protected abstract void probe(RowSequence chunkOk, Chunk[] sourceKeyChunks);

    @Override
    public void remove(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, true, sources, this::probe);
        this.outputPositions = null;
    }

    @Override
    public void findModifications(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, false, sources, this::probe);
        this.outputPositions = null;
    }

    @Override
    public void startTrackingPrevValues() {}
}
