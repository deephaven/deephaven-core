package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.LiveAttributeMap;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link RollupTable} implementation.
 */
public class RollupTableImpl extends HierarchicalTableImpl<RollupTable, RollupTableImpl> implements RollupTable {

    public static final ColumnName KEY_WIDTH_COLUMN_NAME = ColumnName.of("__KEY_WIDTH__");
    public static final ColumnName ROLLUP_COLUMN_NAME = ColumnName.of("__ROLLUP_HIERARCHY__");

    private final Collection<? extends Aggregation> aggregations;
    private final boolean includesConstituents;
    private final Collection<? extends ColumnName> groupByColumns;
    private final RollupNodeOperationsRecorder aggregatedNodeOperations;
    private final RollupNodeOperationsRecorder constituentNodeOperations;

    public RollupTableImpl(
            @NotNull final QueryTable source,
            @NotNull final QueryTable root,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includesConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @Nullable final RollupNodeOperationsRecorder aggregatedNodeOperations,
            @Nullable final RollupNodeOperationsRecorder constituentNodeOperations) {
        super(source, root);
        this.aggregations = aggregations;
        this.includesConstituents = includesConstituents;
        this.groupByColumns = groupByColumns;
        this.aggregatedNodeOperations = aggregatedNodeOperations;
        this.constituentNodeOperations = constituentNodeOperations;
    }

    @Override
    public Collection<? extends Aggregation> getAggregations() {
        return aggregations;
    }

    @Override
    public boolean includesConstituents() {
        return includesConstituents;
    }

    @Override
    public Collection<? extends ColumnName> getGroupByColumns() {
        return groupByColumns;
    }

    @Override
    public ColumnName getKeyWidthColumn() {
        return KEY_WIDTH_COLUMN_NAME;
    }

    @Override
    public ColumnName getRollupColumn() {
        return ROLLUP_COLUMN_NAME;
    }

    @Override
    public Collection<? extends Pair> getColumnPairs() {
        return AggregationPairs.of(aggregations).collect(Collectors.toList());
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder(@NotNull final NodeType nodeType) {
        return new RollupNodeOperationsRecorder(nodeTypeToDefinition(nodeType), nodeType);
    }

    @Override
    public RollupTable withNodeOperations(@NotNull final NodeOperationsRecorder... nodeOperations) {
        if (Stream.of(nodeOperations).allMatch(Objects::isNull)) {
            if (isRefreshing) {
                manageWithCurrentScope();
            }
            return this;
        }
        RollupNodeOperationsRecorder newAggregatedNodeOperations = aggregatedNodeOperations;
        RollupNodeOperationsRecorder newConstituentNodeOperations = constituentNodeOperations;
        for (final NodeOperationsRecorder recorder : nodeOperations) {
            final RollupNodeOperationsRecorder recorderTyped = (RollupNodeOperationsRecorder) recorder;
            switch (recorderTyped.getNodeType()) {
                case Aggregated:
                    newAggregatedNodeOperations = accumulateOperations(newAggregatedNodeOperations, recorderTyped);
                    break;
                case Constituent:
                    assertIncludesConstituents();
                    newConstituentNodeOperations = accumulateOperations(newConstituentNodeOperations, recorderTyped);
                    break;
                default:
                    return unexpectedNodeType(recorderTyped.getNodeType());
            }
        }
        return new RollupTableImpl(source, root, aggregations, includesConstituents, groupByColumns,
                newAggregatedNodeOperations, newConstituentNodeOperations);
    }

    private static RollupNodeOperationsRecorder accumulateOperations(
            @Nullable final RollupNodeOperationsRecorder existing,
            @NotNull final RollupNodeOperationsRecorder added) {
        return existing == null ? added : existing.withOperations(added);
    }

    private TableDefinition nodeTypeToDefinition(@NotNull final NodeType nodeType) {
        switch (nodeType) {
            case Aggregated:
                return root.getDefinition();
            case Constituent:
                assertIncludesConstituents();
                return source.getDefinition();
            default:
                return unexpectedNodeType(nodeType);
        }
    }

    private void assertIncludesConstituents() {
        if (getLeafNodeType() != NodeType.Constituent) {
            throw new IllegalArgumentException("Rollup does not have constituent nodes");
        }
    }

    private static <T> T unexpectedNodeType(@NotNull NodeType nodeType) {
        throw new IllegalArgumentException("Unrecognized node type: " + nodeType);
    }

    @Override
    public RollupTable reapply(@NotNull final UnaryOperator<Table> sourceTransformer) {
        final Table newSource = sourceTransformer.apply(source);
        if (!source.getDefinition().equals(newSource.getDefinition())) {
            throw new UnsupportedOperationException(
                    "Unexpected definition change: " +
                            source.getDefinition().getDifferenceDescription(
                                    newSource.getDefinition(), "original source", "new source", ", "));
        }
        // TODO-RWC: Attribute copies?
        final RollupTable rollup = newSource.rollup(aggregations, includesConstituents, groupByColumns);
        return rollup.withNodeOperations(aggregatedNodeOperations, constituentNodeOperations);
    }

    @Override
    protected RollupTableImpl copy() {
        final RollupTableImpl result =
                new RollupTableImpl(source, root, aggregations, includesConstituents, groupByColumns,
                        aggregatedNodeOperations, constituentNodeOperations);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }
}
