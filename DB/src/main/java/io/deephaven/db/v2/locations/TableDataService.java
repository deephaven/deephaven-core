/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Service responsible for table location discovery.
 */
public interface TableDataService {

    /**
     * Request a table location provider from this service.
     *
     * @param namespace The namespace
     * @param tableName The table name
     * @param tableType The table type
     * @return A table location provider for the specified table key fields
     */
    default @NotNull TableLocationProvider getTableLocationProvider(@NotNull final String namespace,
                                                                    @NotNull final String tableName,
                                                                    @NotNull final TableType tableType) {
        return getTableLocationProvider(new TableLookupKey.Immutable(namespace, tableName, tableType));
    }

    /**
     * Request a table location provider from this service.
     *
     * @param tableKey The table key
     * @return A table location provider for the specified key
     */
    @NotNull TableLocationProvider getTableLocationProvider(@NotNull TableKey tableKey);

    /**
     * Forget all state for future requests.
     */
    void reset();

    /**
     * Forget all state for a single table.
     *
     * @param key
     */
    void reset(TableKey key);

    /**
     * Allow TableDataService instances to have names.
     */
    @Nullable
    String getName();

    /**
     * Like toString, but with more detail.
     * @return a description string
     */
    default String describe() {
        return this.toString();
    }

    class Null implements TableDataService {

        public static final TableDataService INSTANCE = new Null();

        private Null() {
        }

        @Override
        public @NotNull TableLocationProvider getTableLocationProvider(@NotNull final TableKey tableKey) {
            throw new TableDataException("This is the null service");
        }

        @Override
        public void reset() {
            throw new TableDataException("This is the null service");
        }

        @Override
        public void reset(TableKey key) {
            throw new TableDataException("This is the null service");
        }

        @Nullable
        @Override
        public String getName() {
            return null;
        }
    }
}
