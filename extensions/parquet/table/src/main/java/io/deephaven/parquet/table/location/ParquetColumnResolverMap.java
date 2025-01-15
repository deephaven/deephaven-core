//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.annotations.BuildableStyle;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.immutables.value.Value;

import java.util.Map;
import java.util.Optional;

/**
 * A {@link ParquetColumnResolver} implementation based on a map from Deephaven column names to Parquet
 * {@link ColumnDescriptor column descriptors}.
 */
@Value.Immutable
@BuildableStyle
public abstract class ParquetColumnResolverMap implements ParquetColumnResolver {

    public static Builder builder() {
        return ImmutableParquetColumnResolverMap.builder();
    }

    /**
     * The Parquet schema.
     */
    public abstract MessageType schema();

    /**
     * The map from Deephaven column name to {@link ColumnDescriptor}. The {@link #schema()} must contain each column
     * descriptor.
     */
    public abstract Map<String, ColumnDescriptor> mapping();

    @Override
    public final Optional<String[]> of(String columnName) {
        return Optional.ofNullable(mapping().get(columnName)).map(ColumnDescriptor::getPath);
    }

    public interface Builder {
        Builder schema(MessageType schema);

        Builder putMapping(String key, ColumnDescriptor value);

        Builder putAllMapping(Map<String, ? extends ColumnDescriptor> entries);

        ParquetColumnResolverMap build();
    }

    @Value.Check
    final void checkMapping() {
        for (Map.Entry<String, ColumnDescriptor> e : mapping().entrySet()) {
            final ColumnDescriptor columnDescriptor = e.getValue();
            if (!ParquetUtil.contains(schema(), columnDescriptor)) {
                throw new IllegalArgumentException(
                        String.format("schema does not contain Deephaven columnName=%s columnDescriptor=%s", e.getKey(),
                                columnDescriptor));
            }
        }
    }
}
