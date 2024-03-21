//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * A writable source that allows parallel population.
 */
public interface WritableSourceWithPrepareForParallelPopulation {
    /**
     * Does the specified WritableColumnSource provide the prepareForParallelPopulation function?
     *
     * @param wcs the WritableColumnSource to check
     * @return true if prepareForParallelPopulation can be called on wcs
     */
    static boolean supportsParallelPopulation(WritableColumnSource<?> wcs) {
        return wcs instanceof WritableSourceWithPrepareForParallelPopulation;
    }

    /**
     * Prepare this column source such that:
     * <ul>
     * <li>all values in rowSet may be accessed using getPrev</li>
     * <li>all values in rowSet may be populated in parallel</li>
     * </ul>
     * <p>
     * Further operations in this cycle need not check for previous when writing data to the column source; you must
     * provide a row set that contains every row that may be written to this column source.
     *
     * @param rowSequence the row sequence of values that will change on this cycle
     */
    void prepareForParallelPopulation(RowSequence rowSequence);

    /**
     * Test if all {@code sources} support parallel parallel population.
     *
     * @param sources The {@link WritableColumnSource sources} to test
     * @return Whether all {@code sources} support parallel parallel population
     */
    static boolean allSupportParallelPopulation(@NotNull final WritableColumnSource<?>... sources) {
        return Arrays.stream(sources)
                .allMatch(WritableSourceWithPrepareForParallelPopulation::supportsParallelPopulation);
    }

    /**
     * Prepare all {@link WritableColumnSource sources} for parallel population.
     *
     * @param rowSequence The {@link RowSequence} to prepare for
     * @param sources The {@link WritableColumnSource sources} to prepare
     */
    static void prepareAll(
            @NotNull final RowSequence rowSequence,
            @NotNull final WritableColumnSource<?>... sources) {
        for (final WritableColumnSource<?> source : sources) {
            if (!supportsParallelPopulation(source)) {
                throw new IllegalArgumentException(String.format(
                        "WritableColumnSource implementation %s does not support parallel population",
                        source.getClass()));
            }
            ((WritableSourceWithPrepareForParallelPopulation) source).prepareForParallelPopulation(rowSequence);
        }
    }
}
