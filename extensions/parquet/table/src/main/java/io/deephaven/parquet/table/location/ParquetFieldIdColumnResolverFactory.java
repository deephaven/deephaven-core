//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The following is an example {@link ParquetColumnResolver.Factory} that may be useful for testing and debugging
 * purposes, but is not meant to be used for production use cases.
 */
public final class ParquetFieldIdColumnResolverFactory implements ParquetColumnResolver.Factory {

    /**
     * TODO: javadoc
     * 
     * @param columnNameToFieldId a map from Deephaven column names to field ids
     * @return the column resolver provider
     */
    public static ParquetFieldIdColumnResolverFactory of(Map<String, Integer> columnNameToFieldId) {
        return new ParquetFieldIdColumnResolverFactory(columnNameToFieldId
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet()))));
    }

    private final Map<Integer, Set<String>> fieldIdsToDhColumnNames;

    private ParquetFieldIdColumnResolverFactory(Map<Integer, Set<String>> fieldIdsToDhColumnNames) {
        this.fieldIdsToDhColumnNames = Objects.requireNonNull(fieldIdsToDhColumnNames);
    }

    @Override
    public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
        final MessageType schema = tableLocationKey.getFileReader().getSchema();
        // TODO: note the potential for confusion on where to derive schema from.
        // final MessageType schema = tableLocationKey.getMetadata().getFileMetaData().getSchema();
        return of(schema);
    }

    public ParquetColumnResolverMap of(MessageType schema) {
        final FieldIdMappingVisitor visitor = new FieldIdMappingVisitor();
        ParquetUtil.walk(schema, visitor);
        return ParquetColumnResolverMap.builder()
                .schema(schema)
                .putAllMapping(visitor.nameToColumnDescriptor)
                .build();
    }

    private class FieldIdMappingVisitor implements ParquetUtil.Visitor {
        private final Map<String, ColumnDescriptor> nameToColumnDescriptor = new HashMap<>();

        @Override
        public void accept(Collection<Type> path, PrimitiveType primitiveType) {
            // There are different resolution strategies that could all be reasonable. We could consider using only the
            // field id closest to the leaf. This version, however, takes the most general approach and considers field
            // ids wherever they appear; ultimately, only being resolvable if the field id mapping is unambiguous.
            for (Type type : path) {
                final Type.ID id = type.getId();
                if (id == null) {
                    continue;
                }
                final int fieldId = id.intValue();
                final Set<String> set = fieldIdsToDhColumnNames.get(fieldId);
                if (set == null) {
                    continue;
                }
                final ColumnDescriptor columnDescriptor = ParquetUtil.makeColumnDescriptor(path, primitiveType);
                for (String columnName : set) {
                    final ColumnDescriptor existing = nameToColumnDescriptor.putIfAbsent(columnName, columnDescriptor);
                    if (existing != null) {
                        throw new IllegalArgumentException(String.format(
                                "Parquet columns can't be unambigously mapped. %s -> %d has multiple paths %s, %s",
                                columnName, fieldId, Arrays.toString(existing.getPath()),
                                Arrays.toString(columnDescriptor.getPath())));
                    }
                }
            }
        }
    }
}
