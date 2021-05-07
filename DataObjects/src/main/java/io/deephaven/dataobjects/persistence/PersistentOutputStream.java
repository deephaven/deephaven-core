/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects.persistence;

import io.deephaven.dataobjects.AbstractDataObject;
import io.deephaven.dataobjects.DataObjectColumn;
import io.deephaven.dataobjects.DataObjectColumnSet;
import io.deephaven.dataobjects.DataObjectColumnSetManager;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} implementation for backwards-compatible persistence of
 * {@link AbstractDataObject}s when paired with {@link PersistentInputStream}.
 */
@SuppressWarnings("WeakerAccess")
public class PersistentOutputStream extends DataObjectOutputStream {

    private static volatile DataObjectColumnSet[] cachedConvertedColumnSets;

    /**
     * <p>Construct a new PersistentOutputStream with the {@link DataObjectColumnSet}s known to the {@link DataObjectColumnSetManager}
     * singleton prepended for backward-compatible reading.
     * <p>This version of the constructor uses a cache of {@link DataObjectColumnSet}s converted for persistence. Note that this
     * captures the state of the {@link DataObjectColumnSetManager} singleton at the time of caching.
     *
     * @param out The parent stream
     */
    public PersistentOutputStream(@NotNull final OutputStream out) throws IOException {
        super(out);
        writeObject(createConvertedColSets());
    }

    /**
     * <p>Construct a new PersistentOutputStream with the specified {@link DataObjectColumnSet}s prepended for
     * backward-compatible reading.
     * <p>This version of the constructor always converts the supplied {@link DataObjectColumnSet}s for persistence.
     *
     * @param out        The parent stream
     * @param columnSets The {@link DataObjectColumnSet}s to persist
     */
    public PersistentOutputStream(@NotNull final OutputStream out, @NotNull final DataObjectColumnSet... columnSets) throws IOException {
        this(out, true, columnSets);
    }

    /**
     * Construct a new PersistentOutputStream with the specified {@link DataObjectColumnSet}s prepended for backward-compatible
     * reading.
     *
     * @param out              The parent stream
     * @param conversionNeeded Whether the {@link DataObjectColumnSet}s need conversion for persistence
     * @param columnSets       The {@link DataObjectColumnSet}s to persist
     */
    public PersistentOutputStream(@NotNull final OutputStream out,
                                  final boolean conversionNeeded,
                                  @NotNull final DataObjectColumnSet... columnSets) throws IOException {
        super(out);
        writeObject(conversionNeeded ? createConvertedColSets(columnSets) : columnSets);
    }

    /**
     * <p>Convert the {@link DataObjectColumnSet}s known to the {@link DataObjectColumnSetManager} singleton into a format more suitable
     * for persistence.
     * <p>Internally caches results. Note that this captures the state of the {@link DataObjectColumnSetManager} singleton at the
     * time of caching.
     *
     * @return The converted {@link DataObjectColumnSet}s
     */
    public static DataObjectColumnSet[] createConvertedColSets() {
        if (cachedConvertedColumnSets == null) {
            // NB: I could do the usual "double-checked locking" with a synchronized block and ensure only-once
            //     conversion, but I think it might actually be better to let multiple threads do the work if there's a
            //     race, since the results should be identical.
            // TODO: Implement some kind of cache invalidation in case the DataObjectColumnSetManager's column sets change.
            cachedConvertedColumnSets = createConvertedColSets(DataObjectColumnSetManager.getInstance().getColumnSets());
        }
        return cachedConvertedColumnSets;
    }

    /**
     * Convert the specified {@link DataObjectColumnSet}s into a format more suitable for persistence. Does not cache results.
     *
     * @param columnSets The {@link DataObjectColumnSet}s to convert
     * @return The converted {@link DataObjectColumnSet}s
     */
    public static DataObjectColumnSet[] createConvertedColSets(@NotNull final DataObjectColumnSet... columnSets) {
        final DataObjectColumnSet convertedColumnSets[] = new DataObjectColumnSet[columnSets.length];

        for (int i = 0; i < convertedColumnSets.length; i++) {
            final DataObjectColumn columns[] = columnSets[i].getColumns();
            final DataObjectColumn convertedColumns[] = new DataObjectColumn[columns.length];

            for (int j = 0; j < columns.length; j++) {
                convertedColumns[j] = new DataObjectColumn(columns[j].getColumnInfo());

                if (!convertedColumns[j].isBasicType() && !convertedColumns[j].getType().isArray()) {
                    convertedColumns[j].setType(Object.class);
                }
            }

            convertedColumnSets[i] = new DataObjectColumnSet(columnSets[i].getName(), convertedColumns);
        }
        return convertedColumnSets;
    }
}
