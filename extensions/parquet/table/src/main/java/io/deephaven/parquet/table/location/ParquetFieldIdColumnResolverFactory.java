//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.impl.ParquetSchemaUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link ParquetColumnResolver.Factory} resolves Parquet columns via {@link Type#getId() field ids}. The field ids
 * are considered for resolution no matter what level in the schema they exist. For example, the following schema has
 * field ids at different levels:
 *
 * <pre>
 * message root {
 *   required int32 X = 42;
 *   required group Y (LIST) = 43 {
 *     repeated group list {
 *       required int32 element;
 *     }
 *   }
 *   required group Z (LIST) {
 *     repeated group list {
 *       required int32 element = 44;
 *     }
 *   }
 * }
 * </pre>
 *
 * In this example, {@code 42} would be resolvable to {@code [X]}, {@code 43} would be resolvable to
 * {@code [Y, list, element]}, and {@code 44} would be resolvable to {@code [Z, list, element]}.
 *
 * <p>
 * If a schema has ambiguous field ids (according to this implementation's definition), the resolution will fail if the
 * user requests those field ids. For example:
 *
 * <pre>
 * message root {
 *   required int32 X = 42;
 *   required group Y (LIST) = 43 {
 *     repeated group list {
 *       required int32 element;
 *     }
 *   }
 *   required group Z (LIST) {
 *     repeated group list {
 *       required int32 element = 42;
 *     }
 *   }
 * }
 * </pre>
 *
 * In this example, if {@code 42} was requested, resolution would fail because it is ambiguous between paths {@code [X]}
 * and {@code [Z, list, element]}. If {@code 43} was requested, resolution would succeed.
 */
public final class ParquetFieldIdColumnResolverFactory implements ParquetColumnResolver.Factory {

    /**
     * Creates a field id column resolver factory.
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

    /**
     * Resolves the requested field ids for {@code schema}.
     *
     * @param schema the schema
     * @return the resolver map
     */
    public ParquetColumnResolverMap of(MessageType schema) {
        final FieldIdMappingVisitor visitor = new FieldIdMappingVisitor();
        ParquetSchemaUtil.walk(schema, visitor);
        return visitor.toResolver();
    }

    /**
     * Equivalent to {@code of(tableLocationKey.getFileReader().getSchema())}.
     *
     * @param tableKey the table key
     * @param tableLocationKey the Parquet TLK
     * @return the resolver map
     * @see #of(MessageType)
     */
    @Override
    public ParquetColumnResolverMap of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
        return of(tableLocationKey.getSchema());
    }

    private class FieldIdMappingVisitor implements ParquetSchemaUtil.Visitor {
        private final Map<String, List<String>> nameToPath = new HashMap<>();

        public ParquetColumnResolverMap toResolver() {
            ParquetColumnResolverMap.Builder builder = ParquetColumnResolverMap.builder();
            for (Map.Entry<String, List<String>> e : nameToPath.entrySet()) {
                builder.putMap(e.getKey(), e.getValue());
            }
            return builder.build();
        }

        @Override
        public void accept(Collection<Type> typePath, PrimitiveType primitiveType) {
            // There are different resolution strategies that could all be reasonable. We could consider using only the
            // field id closest to the leaf. This version, however, takes the most general approach and considers field
            // ids wherever they appear; ultimately, only being resolvable if the field id mapping is unambiguous.
            List<String> path = null;
            for (Type type : typePath) {
                final Type.ID id = type.getId();
                if (id == null) {
                    continue;
                }
                final int fieldId = id.intValue();
                final Set<String> set = fieldIdsToDhColumnNames.get(fieldId);
                if (set == null) {
                    continue;
                }
                if (path == null) {
                    path = typePath.stream().map(Type::getName).collect(Collectors.toUnmodifiableList());
                }
                for (String columnName : set) {
                    final List<String> existing = nameToPath.putIfAbsent(columnName, path);
                    if (existing != null && !existing.equals(path)) {
                        throw new IllegalArgumentException(String.format(
                                "Parquet columns can't be unambigously mapped. %s -> %d has multiple paths [%s], [%s]",
                                columnName, fieldId, String.join(", ", existing),
                                String.join(", ", path)));
                    }
                }
            }
        }
    }
}
