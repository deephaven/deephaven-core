//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Optional;

/**
 * A resolver from Deephaven column names to Parquet paths.
 */
public interface ParquetColumnResolver {

    /**
     * A factory for creating Parquet column resolvers. This may be useful in situations where the mapping from a
     * Deephaven column name to a Parquet column is not derived directly from the Deephaven column name.
     *
     * @see ParquetInstructions.Builder#setColumnResolverFactory(Factory)
     */
    interface Factory {

        /**
         * Create a Parquet column resolver.
         *
         * @param tableKey the table key
         * @param tableLocationKey the Parquet TLK
         * @return the Parquet column resolver
         */
        ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey);
    }

    /**
     * The path to the leaf field in the Parquet schema corresponding to the Deephaven {@code columnName}.
     *
     * @param columnName the column name
     * @return the path to the leaf field in the Parquet schema
     * @see ColumnDescriptor#getPath()
     * @see MessageType#getColumnDescription(String[])
     */
    Optional<List<String>> of(String columnName);
}
