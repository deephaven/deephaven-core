//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    // region Common HierarchicalTable metadata keys

    /**
     * "hierarchicalTable.isStructuralColumn" is always "true" if set, and is set on columns that should be included on
     * every snapshot or subscription request, but should not be directly user-visible.
     */
    private static final String HIERARCHICAL_TABLE_IS_STRUCTURAL_COLUMN = "hierarchicalTable.isStructuralColumn";
    /**
     * "hierarchicalTable.isExpandByColumn" is always "true" if set, and is set on all the columns that must be included
     * in a HierarchicalTableViewRequest's key table, if a key table is specified. These columns are generally
     * user-visible and displayed before other columns, unless they also have
     * {@value HIERARCHICAL_TABLE_IS_STRUCTURAL_COLUMN} set.
     */
    private static final String HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN = "hierarchicalTable.isExpandByColumn";
    /**
     * "hierarchicalTable.isRowDepthColumn" is always "true" if set, and is set on a single column that specifies the
     * depth of a row. That column will always have {@value #HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN} set for
     * RollupTables, but never for TreeTables.
     */
    private static final String HIERARCHICAL_TABLE_IS_ROW_DEPTH_COLUMN = "hierarchicalTable.isRowDepthColumn";
    /**
     * "hierarchicalTable.isRowExpandedColumn" is always "true" if set, and is set on a single nullable column of
     * booleans that specifies whether a row is expandable or expanded. Values will be null for rows that are not
     * expandable, true for expanded rows, false for rows that are not expanded (but expandable). Leaf rows have no
     * children to expand, and hence will always have a null value for this column.
     */
    private static final String HIERARCHICAL_TABLE_IS_ROW_EXPANDED_COLUMN = "hierarchicalTable.isRowExpandedColumn";

    // endregion Common HierarchicalTable metadata keys

    // region RollupTable metadata keys

    /**
     * "rollupTable.isAggregatedNodeColumn" is always "true" if set, and is set on all columns of a RollupTable that
     * belong to the aggregated nodes.
     */
    private static final String ROLLUP_TABLE_IS_AGGREGATED_NODE_COLUMN = "rollupTable.isAggregatedNodeColumn";
    /**
     * "rollupTable.isConstituentNodeColumn" is always "true" if set, and is set on all columns of a RollupTable that
     * belong to the constituent nodes. No such columns will be present if constituents are not included in the
     * RollupTable.
     */
    private static final String ROLLUP_TABLE_IS_CONSTITUENT_NODE_COLUMN = "rollupTable.isConstituentNodeColumn";
    /**
     * "rollupTable.isGroupByColumn" is always "true" if set, and is set on all columns of a RollupTable that are
     * "group-by columns", whether the node is aggregated or constituent. All nodes have the same names and types for
     * columns labeled in this way. Such columns will always have {@value #HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN} set
     * if and only if they also have {@value #ROLLUP_TABLE_IS_AGGREGATED_NODE_COLUMN} set.
     */
    private static final String ROLLUP_TABLE_IS_GROUP_BY_COLUMN = "rollupTable.isGroupByColumn";
    /**
     * "rollupTable.aggregationInputColumnName" is set to the (string) name of the corresponding constituent column that
     * was used as input to this aggregation node column. May have an empty value, because some aggregations take no
     * input columns, for example "Count". This is only ever present on columns with
     * {@value #ROLLUP_TABLE_IS_AGGREGATED_NODE_COLUMN} set.
     */
    private static final String ROLLUP_TABLE_AGGREGATION_INPUT_COLUMN_NAME = "rollupTable.aggregationInputColumnName";

    // endregion RollupTable metadata keys

    // region TreeTable metadata keys

    /**
     * "treeTable.isNodeColumn" is always "true" if set, and is set on all columns of a TreeTable that nodes inherit
     * from the source Table, as well as their associated formatting columns.
     */
    private static final String TREE_TABLE_IS_NODE_COLUMN = "treeTable.isNodeColumn";
    /**
     * "treeTable.isIdentifierColumn" is always "true" if set, and is set on the single column that uniquely identifies
     * a TreeTable row and links it to its children. Such columns will always have
     * {@value #HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN} set.
     */
    private static final String TREE_TABLE_IS_IDENTIFIER_COLUMN = "treeTable.isIdentifierColumn";
    /**
     * "treeTable.isParentIdentifierColumn" is always "true" if set, and is set on the single column that links a
     * TreeTable row to its parent row.
     */
    private static final String TREE_TABLE_IS_PARENT_IDENTIFIER_COLUMN = "treeTable.isParentIdentifierColumn";

    // endregion TreeTable metadata keys

    /**
     * Re-usable constant for the "true" value.
     */
    private static final String TRUE_STRING = Boolean.toString(true);

    /**
     * Add a schema payload for a {@link HierarchicalTable} to a {@link FlatBufferBuilder}.
     *
     * @param builder The {@link FlatBufferBuilder} to add to
     * @param hierarchicalTable The {@link HierarchicalTable} whose schema should be described
     * @return The offset to the schema payload in the flatbuffer under construction
     */
    public static int makeSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final HierarchicalTable<?> hierarchicalTable) {
        if (hierarchicalTable instanceof RollupTable) {
            final RollupTable rollupTable = (RollupTable) hierarchicalTable;
            return makeRollupTableSchemaPayload(builder, rollupTable);
        }
        if (hierarchicalTable instanceof TreeTable) {
            final TreeTable treeTable = (TreeTable) hierarchicalTable;
            return makeTreeTableSchemaPayload(builder, treeTable);
        }
        throw new IllegalArgumentException("Unknown HierarchicalTable type");
    }

    private static int makeRollupTableSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final RollupTable rollupTable) {

        final Map<String, String> schemaMetadata =
                attributesToMetadata(rollupTable.getAttributes());

        final Set<String> groupByColumns = rollupTable.getGroupByColumns().stream()
                .map(ColumnName::name)
                .collect(Collectors.toSet());

        final Stream<Field> structuralFields = getStructuralFields(rollupTable);
        final Stream<Field> aggregatedFields = getRollupAggregatedNodeFields(rollupTable, groupByColumns);
        final Stream<Field> constituentFields = getRollupConstituentNodeFields(rollupTable, groupByColumns);
        final List<Field> fields = Stream.of(structuralFields, aggregatedFields, constituentFields)
                .flatMap(Function.identity())
                .collect(Collectors.toList());

        return new Schema(fields, schemaMetadata).getSchema(builder);
    }

    private static int makeTreeTableSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final TreeTable treeTable) {

        final Map<String, String> schemaMetadata =
                attributesToMetadata(treeTable.getAttributes());

        final Stream<Field> structuralFields = getStructuralFields(treeTable);
        final Stream<Field> nodeFields = getTreeNodeFields(treeTable);
        final List<Field> fields = Stream.concat(structuralFields, nodeFields).collect(Collectors.toList());

        return new Schema(fields, schemaMetadata).getSchema(builder);
    }

    private static Stream<Field> getStructuralFields(@NotNull final HierarchicalTable<?> hierarchicalTable) {
        return columnDefinitionsToFields(Collections.emptyMap(), null,
                hierarchicalTable.getRoot().getDefinition(),
                hierarchicalTable.getStructuralColumnDefinitions(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, HIERARCHICAL_TABLE_IS_STRUCTURAL_COLUMN, TRUE_STRING);
                    if (columnName.equals(hierarchicalTable.getRowDepthColumn().name())) {
                        putMetadata(metadata, HIERARCHICAL_TABLE_IS_ROW_DEPTH_COLUMN, TRUE_STRING);
                        if (hierarchicalTable instanceof RollupTable) {
                            putMetadata(metadata, HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN, TRUE_STRING);
                        }
                    } else if (columnName.equals(hierarchicalTable.getRowExpandedColumn().name())) {
                        putMetadata(metadata, HIERARCHICAL_TABLE_IS_ROW_EXPANDED_COLUMN, TRUE_STRING);
                    }
                    return metadata;
                }, hierarchicalTable.getAttributes());
    }

    private static Stream<Field> getRollupAggregatedNodeFields(
            @NotNull final RollupTable rollupTable,
            @NotNull final Set<String> groupByColumns) {
        final Map<String, String> aggregationColumnToInputColumnName =
                AggregationPairs.of(rollupTable.getAggregations())
                        .collect(Collectors.toMap(cnp -> cnp.output().name(), cnp -> cnp.input().name()));
        final Map<String, String> aggregatedNodeDescriptions = getColumnDescriptions(rollupTable.getAttributes());
        return columnDefinitionsToFields(aggregatedNodeDescriptions, null,
                rollupTable.getNodeDefinition(RollupTable.NodeType.Aggregated),
                rollupTable.getNodeDefinition(RollupTable.NodeType.Aggregated).getColumns(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, ROLLUP_TABLE_IS_AGGREGATED_NODE_COLUMN, TRUE_STRING);
                    if (groupByColumns.contains(columnName)) {
                        putMetadata(metadata, ROLLUP_TABLE_IS_GROUP_BY_COLUMN, TRUE_STRING);
                        putMetadata(metadata, HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN, TRUE_STRING);
                    } else {
                        putMetadata(metadata, ROLLUP_TABLE_AGGREGATION_INPUT_COLUMN_NAME,
                                aggregationColumnToInputColumnName.getOrDefault(columnName, ""));
                    }
                    return metadata;
                }, rollupTable.getAttributes());
    }

    private static Stream<Field> getRollupConstituentNodeFields(
            @NotNull final RollupTable rollupTable,
            @NotNull final Set<String> groupByColumns) {
        if (!rollupTable.includesConstituents()) {
            return Stream.empty();
        }
        final Map<String, Object> sourceAttributes = rollupTable.getSource().getAttributes();
        final Map<String, String> constituentNodeDescriptions = getColumnDescriptions(sourceAttributes);
        return columnDefinitionsToFields(constituentNodeDescriptions, null,
                rollupTable.getNodeDefinition(RollupTable.NodeType.Constituent),
                rollupTable.getNodeDefinition(RollupTable.NodeType.Constituent).getColumns(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, ROLLUP_TABLE_IS_CONSTITUENT_NODE_COLUMN, TRUE_STRING);
                    if (groupByColumns.contains(columnName)) {
                        putMetadata(metadata, ROLLUP_TABLE_IS_GROUP_BY_COLUMN, TRUE_STRING);
                    }
                    return metadata;
                }, rollupTable.getAttributes());
    }

    private static Stream<Field> getTreeNodeFields(@NotNull final TreeTable treeTable) {
        final Map<String, String> treeNodeDescriptions = getColumnDescriptions(treeTable.getAttributes());
        return columnDefinitionsToFields(treeNodeDescriptions, null,
                treeTable.getNodeDefinition(),
                treeTable.getNodeDefinition().getColumns(),
                columnName -> {
                    final Map<String, String> metadata = new HashMap<>();
                    putMetadata(metadata, TREE_TABLE_IS_NODE_COLUMN, TRUE_STRING);
                    if (columnName.equals(treeTable.getIdentifierColumn().name())) {
                        putMetadata(metadata, TREE_TABLE_IS_IDENTIFIER_COLUMN, TRUE_STRING);
                        putMetadata(metadata, HIERARCHICAL_TABLE_IS_EXPAND_BY_COLUMN, TRUE_STRING);
                    } else if (columnName.equals(treeTable.getParentIdentifierColumn().name())) {
                        putMetadata(metadata, TREE_TABLE_IS_PARENT_IDENTIFIER_COLUMN, TRUE_STRING);
                    }
                    return metadata;
                }, treeTable.getAttributes());
    }
}
