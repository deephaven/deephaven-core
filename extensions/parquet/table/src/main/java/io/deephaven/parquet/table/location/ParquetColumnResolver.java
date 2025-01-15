//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Optional;

/**
 * A mapping between Deephaven column names and Parquet {@link ColumnDescriptor column descriptors}.
 *
 * TODO: describe better
 */
public interface ParquetColumnResolver {

    /**
     * {@link ParquetInstructions.Builder#setColumnResolverFactory(Factory)}
     */
    interface Factory {

        /**
         * TODO: description
         *
         * @param tableKey the table key
         * @param tableLocationKey the Parquet TLK
         * @return the Parquet column resolver
         */
        ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey);
    }

    Optional<ColumnDescriptor> of(String columnName);
}
