//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.immutables.value.Value;

import java.util.Map;

/**
 * A mapping between Deephaven column names and Parquet {@link ColumnDescriptor column descriptors}.
 *
 * TODO: describe better
 */
@Value.Immutable
@BuildableStyle
public abstract class ParquetColumnResolver {

    /**
     * {@link ParquetInstructions.Builder#setColumnResolverFactory(Factory)}
     */
    public interface Factory {

        /**
         * TODO: description
         *
         * @param tableKey the table key
         * @param tableLocationKey the Parquet TLK
         * @return the Parquet column resolver
         */
        ParquetColumnResolver init(TableKey tableKey, ParquetTableLocationKey tableLocationKey);
    }

    public static Builder builder() {
        return ImmutableParquetColumnResolver.builder();
    }

    // Intentionally not exposed, but necessary to expose to Builder for safety checks.
    abstract MessageType schema();

    /**
     * TODO: javadoc
     * 
     * @return
     */
    public abstract Map<String, ColumnDescriptor> mapping();

    @Value.Check
    final void checkColumns() {
        for (ColumnDescriptor columnDescriptor : mapping().values()) {
            if (!ParquetUtil.contains(schema(), columnDescriptor)) {
                throw new IllegalArgumentException("schema does not contain column descriptor " + columnDescriptor);
            }
        }
    }

    public interface Builder {

        // TODO: javadoc

        Builder schema(MessageType schema);

        Builder putMapping(String key, ColumnDescriptor value);

        Builder putAllMapping(Map<String, ? extends ColumnDescriptor> entries);

        ParquetColumnResolver build();
    }
}
