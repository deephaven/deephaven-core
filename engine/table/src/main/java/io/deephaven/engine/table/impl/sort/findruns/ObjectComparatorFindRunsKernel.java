//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.findruns;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * A {@link FindRunsKernel} for Object values sorted with a {@link Comparator}. A run is a sequence of values for which
 * the comparator returns zero, so the runs are exactly the ties of a sort that used the same comparator.
 */
public class ObjectComparatorFindRunsKernel implements FindRunsKernel {
    private final Comparator<Object> comparator;

    /**
     * Create a kernel that finds runs of values that {@code comparator} considers equal. Runs are the same whether the
     * values were sorted ascending or descending by {@code comparator}; {@link #compactRuns} verifies ascending order.
     *
     * @param comparator the comparator whose ties form the runs
     */
    public ObjectComparatorFindRunsKernel(@NotNull final Comparator<?> comparator) {
        // noinspection unchecked
        this.comparator = (Comparator<Object>) comparator;
    }

    @Override
    public void findRuns(
            @NotNull final Chunk<? extends Any> sortedValues,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues.asObjectChunk(), 0, sortedValues.size(), offsetsOut, lengthsOut, false);
    }

    @Override
    public void findRunsSingles(
            @NotNull final Chunk<? extends Any> sortedValues,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        findRuns(sortedValues.asObjectChunk(), 0, sortedValues.size(), offsetsOut, lengthsOut, true);
    }

    @Override
    public void findRuns(
            @NotNull final Chunk<? extends Any> sortedValues,
            @NotNull final IntChunk<ChunkPositions> offsetsIn,
            @NotNull final IntChunk<ChunkLengths> lengthsIn,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut) {
        offsetsOut.setSize(0);
        lengthsOut.setSize(0);

        final ObjectChunk<Object, ? extends Any> objectValues = sortedValues.asObjectChunk();
        final int numberRuns = offsetsIn.size();
        for (int run = 0; run < numberRuns; ++run) {
            findRuns(objectValues, offsetsIn.get(run), lengthsIn.get(run), offsetsOut, lengthsOut, false);
        }
    }

    private void findRuns(
            @NotNull final ObjectChunk<Object, ? extends Any> sortedValues,
            final int offset,
            final int length,
            @NotNull final WritableIntChunk<ChunkPositions> offsetsOut,
            @NotNull final WritableIntChunk<ChunkLengths> lengthsOut,
            final boolean includeSingles) {
        if (length == 0) {
            return;
        }
        int startRun = offset;
        int cursor = startRun;
        Object last = sortedValues.get(cursor++);
        while (cursor < offset + length) {
            final Object next = sortedValues.get(cursor);
            if (comparator.compare(last, next) != 0) {
                if (includeSingles || cursor != startRun + 1) {
                    offsetsOut.add(startRun);
                    lengthsOut.add(cursor - startRun);
                }
                startRun = cursor;
            }
            cursor++;
            last = next;
        }
        if (includeSingles || cursor != startRun + 1) {
            offsetsOut.add(startRun);
            lengthsOut.add(cursor - startRun);
        }
    }

    @Override
    public int compactRuns(
            @NotNull final WritableChunk<? extends Any> sortedValues,
            @NotNull final IntChunk<ChunkPositions> offsetsIn) {
        final WritableObjectChunk<Object, ? extends Any> objectValues = sortedValues.asWritableObjectChunk();
        final int numRuns = offsetsIn.size();
        if (numRuns == 0) {
            return -1;
        }
        Object last = objectValues.get(offsetsIn.get(0));
        objectValues.set(0, last);
        for (int ri = 1; ri < numRuns; ++ri) {
            final Object next = objectValues.get(offsetsIn.get(ri));
            if (comparator.compare(next, last) <= 0) {
                return ri;
            }
            objectValues.set(ri, next);
            last = next;
        }
        objectValues.setSize(numRuns);
        return -1;
    }
}
