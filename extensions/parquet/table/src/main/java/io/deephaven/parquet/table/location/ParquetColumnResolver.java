//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.util.Optional;

/**
 * A resolver from Deephaven column names to Parquet paths.
 */
public interface ParquetColumnResolver {

    /**
     * {@link ParquetInstructions.Builder#setColumnResolverFactory(Factory)}
     */
    interface Factory {

        /**
         * TODO: description
         *
         *
         *
         * @param tableKey the table key
         * @param tableLocationKey the Parquet TLK
         * @return the Parquet column resolver
         */
        ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey);
    }

    /**
     *
     *
     * @param columnName the column name
     * @return the path to the leaf field in the schema
     * @see ColumnDescriptor#getPath()
     * @see MessageType#getColumnDescription(String[])
     */
    Optional<String[]> of(String columnName);
}
