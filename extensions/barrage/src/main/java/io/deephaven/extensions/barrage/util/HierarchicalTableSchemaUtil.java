package io.deephaven.extensions.barrage.util;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.table.GridAttributes.getColumnDescriptions;
import static io.deephaven.extensions.barrage.util.BarrageUtil.*;

/**
 * Utilities for creating schemas from {@link HierarchicalTable} instances.
 */
public class HierarchicalTableSchemaUtil {

    private static final String TRUE_STRING = Boolean.toString(true);

    public static int makeSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final HierarchicalTable hierarchicalTable) {
        if (hierarchicalTable instanceof RollupTable) {
            final RollupTable rollupTable = (RollupTable) hierarchicalTable;
            return HierarchicalTableSchemaUtil.makeRollupTableSchemaPayload(builder, rollupTable);
        } else if (hierarchicalTable instanceof TreeTable) {
            final TreeTable treeTable = (TreeTable) hierarchicalTable;
            return HierarchicalTableSchemaUtil.makeTreeTableSchemaPayload(builder, treeTable);
        } else {
            throw new IllegalArgumentException("Unknown HierarchicalTable type");
        }
    }

    private static int makeRollupTableSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final RollupTable rollupTable) {
        final Map<String, Object> rollupAttributes = rollupTable.getAttributes();

        final Map<String, String> schemaMetadata = attributesToMetadata(rollupAttributes);

        final Set<String> groupByColumns = rollupTable.getGroupByColumns().stream()
                .map(ColumnName::name)
                .collect(Collectors.toSet());
        final Map<String, String> aggregationColumnToInputColumnName =
                AggregationPairs.of(rollupTable.getAggregations())
                        .collect(Collectors.toMap(cnp -> cnp.output().name(), cnp -> cnp.input().name()));

        final Stream<Field> structuralFields = getStructuralFields(rollupTable);
        final Stream<Field> aggregatedFields;
        {
            final Map<String, String> aggregatedNodeDescriptions = getColumnDescriptions(rollupAttributes);
            aggregatedFields = tableDefinitionToFields(aggregatedNodeDescriptions, null,
                    rollupTable.getNodeDefinition(RollupTable.NodeType.Aggregated),
                    columnName -> {
                        final Map<String, String> metadata = new HashMap<>();
                        putMetadata(metadata, "rollupTable.isAggregatedNodeColumn", TRUE_STRING);
                        if (groupByColumns.contains(columnName)) {
                            putMetadata(metadata, "rollupTable.isGroupByColumn", TRUE_STRING);
                            putMetadata(metadata, "hierarchicalTable.isExpandByColumn", TRUE_STRING);
                        } else {
                            putMetadata(metadata, "rollupTable.aggregationInputColumnName",
                                    aggregationColumnToInputColumnName.getOrDefault(columnName, ""));
                        }
                        return metadata;
                    });
        }
        final Stream<Field> constituentFields;
        if (rollupTable.includesConstituents()) {
            final Map<String, Object> sourceAttributes = rollupTable.getSource().getAttributes();
            final Map<String, String> constituentNodeDescriptions = getColumnDescriptions(sourceAttributes);
            constituentFields = tableDefinitionToFields(constituentNodeDescriptions, null,
                    rollupTable.getNodeDefinition(RollupTable.NodeType.Constituent),
                    columnName -> {
                        final Map<String, String> metadata = new HashMap<>();
                        putMetadata(metadata, "rollupTable.isConstituentNodeColumn", TRUE_STRING);
                        if (groupByColumns.contains(columnName)) {
                            putMetadata(metadata, "rollupTable.isGroupByColumn", TRUE_STRING);
                        }
                        return metadata;
                    });
        } else {
            constituentFields = Stream.empty();
        }

        final List<Field> fields = Stream.of(structuralFields, aggregatedFields, constituentFields)
                .flatMap(Function.identity())
                .collect(Collectors.toList());

        return new Schema(fields, schemaMetadata).getSchema(builder);
    }

    private static int makeTreeTableSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final TreeTable treeTable) {
        final Map<String, Object> treeAttributes = treeTable.getAttributes();

        final Map<String, String> schemaMetadata = attributesToMetadata(treeAttributes);

        final Stream<Field> structuralFields = getStructuralFields(treeTable);

        final Map<String, String> descriptions = getColumnDescriptions(treeAttributes);
        final Stream<Field> nodeFields = tableDefinitionToFields(descriptions, null, treeTable.getNodeDefinition(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, "treeTable.isNodeColumn", TRUE_STRING);
                    if (columnName.equals(treeTable.getIdentifierColumn().name())) {
                        putMetadata(metadata, "treeTable.isIdentifierColumn", TRUE_STRING);
                        putMetadata(metadata, "hierarchicalTable.isExpandByColumn", TRUE_STRING);
                    } else if (columnName.equals(treeTable.getParentIdentifierColumn().name())) {
                        putMetadata(metadata, "treeTable.isParentIdentifierColumn", TRUE_STRING);
                    }
                    return metadata;
                });

        final List<Field> fields = Stream.concat(structuralFields, nodeFields).collect(Collectors.toList());

        return new Schema(fields, schemaMetadata).getSchema(builder);
    }

    private static Stream<Field> getStructuralFields(@NotNull HierarchicalTable hierarchicalTable) {
        return tableDefinitionToFields(Collections.emptyMap(), null,
                hierarchicalTable.getStructuralDefinition(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, "hierarchicalTable.isStructuralColumn", TRUE_STRING);
                    if (columnName.equals(hierarchicalTable.getRowDepthColumn().name())) {
                        putMetadata(metadata, "hierarchicalTable.isRowDepthColumn", TRUE_STRING);
                        if (hierarchicalTable instanceof RollupTable) {
                            putMetadata(metadata, "hierarchicalTable.isExpandByColumn", TRUE_STRING);
                        }
                    } else if (columnName.equals(hierarchicalTable.getRowExpandedColumn().name())) {
                        putMetadata(metadata, "hierarchicalTable.isRowExpandedColumn", TRUE_STRING);
                    }
                    return metadata;
                });
    }
}
