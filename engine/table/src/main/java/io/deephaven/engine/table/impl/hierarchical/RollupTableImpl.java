package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.LiveAttributeMap;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RollupInfo;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.api.ColumnName.names;
import static io.deephaven.engine.table.impl.by.AggregationContextTransformer.UNKNOWN_ROW;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;

/**
 * {@link RollupTable} implementation.
 */
public class RollupTableImpl extends HierarchicalTableImpl<RollupTable, RollupTableImpl> implements RollupTable {

    public static final ColumnName KEY_WIDTH_COLUMN = ColumnName.of("__KEY_WIDTH__");
    public static final ColumnName ROLLUP_COLUMN = ColumnName.of("__ROLLUP_HIERARCHY__");

    private final Collection<? extends Aggregation> aggregations;
    private final boolean includesConstituents;
    private final Collection<? extends ColumnName> groupByColumns;

    final int baseLevel;
    final int numLevels;
    private final QueryTable[] levelTables;
    private final ToIntFunction<Object>[] levelRowLookups;

    private final RollupNodeOperationsRecorder aggregatedNodeOperations;
    private final RollupNodeOperationsRecorder constituentNodeOperations;

    private RollupTableImpl(
            @NotNull final QueryTable source,
            @NotNull final QueryTable root,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includesConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns,
            @NotNull final QueryTable[] levelTables,
            @NotNull final ToIntFunction<Object>[] levelRowLookups,
            @Nullable final RollupNodeOperationsRecorder aggregatedNodeOperations,
            @Nullable final RollupNodeOperationsRecorder constituentNodeOperations) {
        super(source, root);
        this.aggregations = aggregations;
        this.includesConstituents = includesConstituents;
        this.groupByColumns = groupByColumns;

        baseLevel = groupByColumns.size();
        numLevels = baseLevel + 1;
        Require.eq(numLevels, "level count", levelTables.length, "levelTables.length");
        this.levelTables = levelTables;
        Require.eq(numLevels, "level count", levelRowLookups.length, "levelRowLookups.length");
        this.levelRowLookups = levelRowLookups;

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
        return KEY_WIDTH_COLUMN;
    }

    @Override
    public ColumnName getRollupColumn() {
        return ROLLUP_COLUMN;
    }

    @Override
    public Collection<? extends Pair> getColumnPairs() {
        return AggregationPairs.of(aggregations).collect(Collectors.toList());
    }

    @Override
    public RollupTable withFilters(@NotNull final Collection<? extends Filter> filters) {
        final WhereFilter[] whereFilters = initializeAndValidateFilters(filters);
        final QueryTable filteredBaseLevel = (QueryTable) levelTables[baseLevel].where(whereFilters);
        final ToIntFunction<Object> baseLevelRowLookup = levelRowLookups[baseLevel];
        final RowSet filteredBaseLevelRowSet = filteredBaseLevel.getRowSet();
        final ToIntFunction<Object> filteredBaseLevelRowLookup = key -> {
            final int unfilteredRowKey = baseLevelRowLookup.applyAsInt(key);
            // NB: Rollup snapshot patterns allow us to safely always use current, here.
            if (filteredBaseLevelRowSet.find(unfilteredRowKey) > 0) {
                return unfilteredRowKey;
            }
            return UNKNOWN_ROW;
        };
        return makeRollupFromBaseLevel(
                filteredBaseLevel, filteredBaseLevelRowLookup, aggregations, includesConstituents, groupByColumns)
                        .withNodeOperations(aggregatedNodeOperations, constituentNodeOperations);
    }

    private WhereFilter[] initializeAndValidateFilters(@NotNull final Collection<? extends Filter> filters) {
        final WhereFilter[] whereFilters = WhereFilter.from(filters);
        for (final WhereFilter whereFilter : whereFilters) {
            whereFilter.init(source.getDefinition());
            final List<String> invalidColumnsUsed = whereFilter.getColumns().stream().map(ColumnName::of)
                    .filter(cn -> !groupByColumns.contains(cn)).map(ColumnName::name).collect(Collectors.toList());
            if (!invalidColumnsUsed.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid filter found: " + whereFilter + " may only use group-by columns, which are "
                                + names(groupByColumns) + ", but has also used " + invalidColumnsUsed);
            }
            final boolean usesArrays = !whereFilter.getColumnArrays().isEmpty();
            if (usesArrays) {
                throw new IllegalArgumentException("Invalid filter found: " + whereFilter
                        + " may not use column arrays, but uses column arrays from " + whereFilter.getColumnArrays());
            }
        }
        return whereFilters;
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
        return new RollupTableImpl(source, root, aggregations, includesConstituents, groupByColumns, levelTables,
                levelRowLookups, newAggregatedNodeOperations, newConstituentNodeOperations);
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
    protected RollupTableImpl copy() {
        final RollupTableImpl result =
                new RollupTableImpl(source, root, aggregations, includesConstituents, groupByColumns, levelTables,
                        levelRowLookups, aggregatedNodeOperations, constituentNodeOperations);
        LiveAttributeMap.copyAttributes(this, result, ak -> true);
        return result;
    }

    public static RollupTable makeRollup(
            @NotNull final QueryTable input,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final QueryTable baseLevel = input.aggNoMemo(
                AggregationProcessor.forRollupBase(aggregations, includeConstituents), false, null, groupByColumns);
        return makeRollupFromBaseLevel(
                baseLevel, getRowLookup(baseLevel), aggregations, includeConstituents, groupByColumns);

    }

    private static RollupTable makeRollupFromBaseLevel(
            @NotNull final QueryTable baseLevel,
            @NotNull final ToIntFunction<Object> baseLevelRowLookup,
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents,
            @NotNull final Collection<? extends ColumnName> groupByColumns) {
        final Deque<ColumnName> columnsToReaggregate = new ArrayDeque<>(groupByColumns);
        final Deque<String> nullColumnNames = new ArrayDeque<>(groupByColumns.size());
        QueryTable lastLevel = baseLevel;
        while (!columnsToReaggregate.isEmpty()) {
            nullColumnNames.addFirst(columnsToReaggregate.removeLast().name());
            final TableDefinition lastLevelDefinition = lastLevel.getDefinition();
            final Map<String, Class<?>> nullColumns = nullColumnNames.stream().collect(Collectors.toMap(
                    Function.identity(), ncn -> lastLevelDefinition.getColumn(ncn).getDataType(),
                    Assert::neverInvoked, LinkedHashMap::new));
            lastLevel = lastLevel.aggNoMemo(
                    AggregationProcessor.forRollupReaggregated(aggregations, nullColumns), false, null,
                    new ArrayList<>(columnsToReaggregate));
        }

        final String[] internalColumnsToDrop = lastLevel.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).toArray(String[]::new);
        final QueryTable finalTable = (QueryTable) lastLevel.dropColumns(internalColumnsToDrop);
        final Object reverseLookup =
                Require.neqNull(lastLevel.getAttribute(REVERSE_LOOKUP_ATTRIBUTE), "REVERSE_LOOKUP_ATTRIBUTE");
        finalTable.setAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, reverseLookup);

        final Table result = BaseHierarchicalTable.createFrom(finalTable, new RollupInfo(aggregations, groupByColumns,
                includeConstituents ? RollupInfo.LeafType.Constituent : RollupInfo.LeafType.Normal));
        result.setAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE, QueryTable.this);
        copyAttributes(result, BaseTable.CopyAttributeOperation.Rollup);
        maybeUpdateSortableColumns(result);

        return result;
    }
}
