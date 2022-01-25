package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.rollup.NullColumns;
import io.deephaven.engine.table.impl.by.rollup.Partition;
import io.deephaven.engine.table.impl.by.rollup.RollupAggregation;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SingleValueObjectColumnSource;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.engine.table.Table.REVERSE_LOOKUP_ATTRIBUTE;
import static io.deephaven.engine.table.impl.RollupInfo.ROLLUP_COLUMN;
import static io.deephaven.engine.table.impl.by.IterativeOperatorSpec.*;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;

/**
 * Conversion tool to generate an {@link AggregationContextFactory} for a collection of {@link Aggregation
 * aggregations}.
 */
public class AggregationProcessor implements AggregationContextFactory {

    /**
     * Convert a collection of {@link Aggregation aggregations} to an {@link AggregationContextFactory}.
     *
     * @param aggregations The {@link Aggregation aggregations}
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory of(@NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Type type) {
        return new AggregationProcessor(aggregations, type);
    }

    public enum Type {
        STANDARD, ROLLUP_BASE, ROLLUP_REAGGREGATED
    }

    private final Collection<? extends Aggregation> aggregations;
    private final Type type;

    private AggregationProcessor(
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Type type) {
        this.aggregations = aggregations;
        this.type = type;
        final String duplicationErrorMessage = AggregationOutputs.of(aggregations)
                .collect(Collectors.groupingBy(ColumnName::name, Collectors.counting())).entrySet().stream()
                .filter(kv -> kv.getValue() > 1).map(kv -> kv.getKey() + " used " + kv.getValue() + " times")
                .collect(Collectors.joining(", "));
        if (!duplicationErrorMessage.isBlank()) {
            throw new IllegalArgumentException("Duplicate output columns found: " + duplicationErrorMessage);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // AggregationContextFactory
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public AggregationContext makeAggregationContext(@NotNull Table table, @NotNull String... groupByColumnNames) {
        switch (type) {
            case STANDARD:
                return new StandardConverter(table, groupByColumnNames).build();
            case ROLLUP_BASE:
                return new RollupBaseConverter(table, groupByColumnNames).build();
            case ROLLUP_REAGGREGATED:
                return new RollupReaggregatedConverter(table, groupByColumnNames).build();
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Converter Framework
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Base class for conversion from a collection of {@link Aggregation aggregations} to an {@link AggregationContext}
     * for {@code aggregations}. Accumulates state by visiting each aggregation.
     */
    private abstract class Converter implements Aggregation.Visitor, AggSpec.Visitor {

        final QueryTable table;
        final String[] groupByColumnNames;

        final boolean isAddOnly;
        final boolean isStream;

        final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
        final List<String[]> inputColumnNames = new ArrayList<>();
        final List<ChunkSource.WithPrev<Values>> inputSources = new ArrayList<>();
        final List<AggregationContextTransformer> transformers = new ArrayList<>();

        List<Pair> resultPairs = List.of();
        int trackedFirstOrLastIndex = -1;

        private Converter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            this.table = (QueryTable) table.coalesce();
            this.groupByColumnNames = groupByColumnNames;
            isAddOnly = this.table.isAddOnly();
            isStream = this.table.isStream();
        }

        AggregationContext build() {
            for (final Aggregation aggregation : aggregations) {
                aggregation.walk(this);
            }
            // noinspection unchecked
            return new AggregationContext(
                    operators.toArray(IterativeChunkedAggregationOperator[]::new),
                    inputColumnNames.toArray(String[][]::new),
                    inputSources.toArray(ChunkSource.WithPrev[]::new),
                    transformers.toArray(AggregationContextTransformer[]::new));
        }

        final void addNoInputOperator(@NotNull final IterativeChunkedAggregationOperator operator) {
            operators.add(operator);
            inputColumnNames.add(ZERO_LENGTH_STRING_ARRAY);
            inputSources.add(null);
        }

        void streamUnsupported(@NotNull final String operationName) {
            if (!isStream) {
                return;
            }
            throw new UnsupportedOperationException(String.format(
                    "Stream tables do not support Agg%s; use StreamTableTools.streamToAppendOnlyTable to accumulate full history",
                    operationName));
        }

        // -------------------------------------------------------------------------------------------------------------
        // Partial Aggregation.Visitor (for cases common to all types)
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public final void visit(@NotNull final Count count) {
            addNoInputOperator(new CountAggregationOperator(count.column().name()));
        }

        // -------------------------------------------------------------------------------------------------------------
        // Partial AggSpec.Visitor (for cases common to all types)
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public final void visit(@NotNull final ColumnAggregation columnAgg) {
            resultPairs = List.of(columnAgg.pair());
            columnAgg.spec().walk(this);
            resultPairs = List.of();
        }

        @Override
        public final void visit(@NotNull final ColumnAggregations columnAggs) {
            resultPairs = columnAggs.pairs();
            columnAggs.spec().walk(this);
            resultPairs = List.of();
        }

        // -------------------------------------------------------------------------------------------------------------
        // Helpers for visitors
        // -------------------------------------------------------------------------------------------------------------

        void visitBasicAgg(BiFunction<Class<?>, String, IterativeChunkedAggregationOperator> operatorFactory) {
            for (final Pair pair : resultPairs) {
                final String inputName = pair.input().name();
                final String resultName = pair.output().name();
                final ColumnSource<?> rawInputSource = table.getColumnSource(inputName);
                final Class<?> type = rawInputSource.getType();
                final ColumnSource<?> inputSource = maybeReinterpretDateTimeAsLong(rawInputSource);

                operators.add(operatorFactory.apply(type, resultName));
                inputColumnNames.add(new String[] {inputName});
                inputSources.add(inputSource);
            }
        }

        void visitMinOrMaxAgg(final boolean isMin) {
            for (final Pair pair : resultPairs) {
                final String inputName = pair.input().name();
                final String resultName = pair.output().name();
                final ColumnSource<?> rawInputSource = table.getColumnSource(inputName);
                final Class<?> type = rawInputSource.getType();
                final ColumnSource<?> inputSource = maybeReinterpretDateTimeAsLong(rawInputSource);

                IntStream.range(0, inputSources.size())
                        .filter(index -> (inputSources.get(index) == inputSource)
                                && (operators.get(index) instanceof SsmChunkedMinMaxOperator))
                        .findFirst().ifPresentOrElse(
                                (final int priorMinMaxIndex) -> {
                                    final SsmChunkedMinMaxOperator ssmChunkedMinMaxOperator =
                                            (SsmChunkedMinMaxOperator) operators.get(priorMinMaxIndex);
                                    operators.add(ssmChunkedMinMaxOperator.makeSecondaryOperator(isMin, resultName));
                                    inputColumnNames.add(new String[] {inputName});
                                    inputSources.add(null);
                                },
                                () -> {
                                    operators.add(getMinMaxChunked(type, resultName, isMin, isAddOnly || isStream));
                                    inputColumnNames.add(new String[] {inputName});
                                    inputSources.add(inputSource);
                                });
            }
        }

        void visitFirstOrLastAgg(final boolean isFirst, final String exposeRedirectionAs) {
            if (exposeRedirectionAs != null) {
                streamUnsupported((isFirst ? "First" : "Last") +
                        " with exposed row redirections (e.g. for rollup(), AggFirstRowKey, or AggLastRowKey)");
            }
            final MatchPair[] resultMatchPairs = MatchPair.fromPairs(resultPairs);
            final IterativeChunkedAggregationOperator operator;
            if (table.isRefreshing()) {
                if (isAddOnly) {
                    operator = new AddOnlyFirstOrLastChunkedOperator(isFirst, resultMatchPairs, table,
                            exposeRedirectionAs);
                } else if (isStream) {
                    operator = isFirst
                            ? new StreamFirstChunkedOperator(resultMatchPairs, table)
                            : new StreamLastChunkedOperator(resultMatchPairs, table);
                } else {
                    if (trackedFirstOrLastIndex >= 0) {
                        operator = ((FirstOrLastChunkedOperator) operators.get(trackedFirstOrLastIndex))
                                .makeSecondaryOperator(isFirst, resultMatchPairs, table, exposeRedirectionAs);
                    } else {
                        trackedFirstOrLastIndex = operators.size();
                        operator = new FirstOrLastChunkedOperator(isFirst, resultMatchPairs, table,
                                exposeRedirectionAs);
                    }
                }
            } else {
                operator = new StaticFirstOrLastChunkedOperator(isFirst, resultMatchPairs, table, exposeRedirectionAs);
            }
            addNoInputOperator(operator);
        }

        void visitSortedFirstOrLastAgg(@NotNull final List<SortColumn> sortColumns, final boolean isFirst) {
            final String[] sortColumnNames = sortColumns.stream().map(sc -> {
                descendingSortedFirstOrLastUnsupported(sc, isFirst);
                return sc.column().name();
            }).toArray(String[]::new);
            final ChunkSource.WithPrev<Values> inputSource;
            if (sortColumnNames.length == 1) {
                inputSource = table.getColumnSource(sortColumnNames[0]);
            } else {
                // Create a tuple source, because our underlying SSA does not handle multiple sort columns
                inputSource = TupleSourceFactory.makeTupleSource(
                        Arrays.stream(sortColumnNames).map(table::getColumnSource).toArray(ColumnSource[]::new));
            }
            // TODO-RWC: Move this helper here or to a new landing place
            operators.add(SortedFirstOrLastByAggregationFactory.makeOperator(inputSource.getChunkType(), isFirst,
                    aggregations.size() > 1, MatchPair.fromPairs(resultPairs), table));
            inputColumnNames.add(sortColumnNames);
            inputSources.add(inputSource);
        }

        void descendingSortedFirstOrLastUnsupported(@NotNull final SortColumn sortColumn, final boolean isFirst) {
            if (sortColumn.order() == SortColumn.Order.ASCENDING) {
                return;
            }
            throw new UnsupportedOperationException(String.format("%s does not support sort order in %s",
                    isFirst ? "SortedFirst" : "SortedLast", sortColumn));
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Standard Aggregations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext} for standard aggregations. Accumulates state by visiting each aggregation.
     */
    private class StandardConverter extends Converter {

        private StandardConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            super(table, groupByColumnNames);
        }

        // -------------------------------------------------------------------------------------------------------------
        // Aggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final FirstRowKey firstRowKey) {
            visitFirstOrLastAgg(true, firstRowKey.column().name());
        }

        @Override
        public void visit(@NotNull final LastRowKey lastRowKey) {
            visitFirstOrLastAgg(false, lastRowKey.column().name());
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            // TODO-RWC: Move this helper and its friends here or to a new landing place
            visitBasicAgg(IterativeOperatorSpec::getAbsSumChunked);
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            visitBasicAgg((t, n) -> getCountDistinctChunked(t, n, countDistinct.countNulls(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            visitBasicAgg((t, n) -> getDistinctChunked(t, n, distinct.includeNulls(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecGroup group) {
            streamUnsupported("Group");
            addNoInputOperator(new GroupByChunkedOperator(table, true, MatchPair.fromPairs(resultPairs)));
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            visitBasicAgg((t, n) -> getAvgChunked(t, n, false));
        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {
            visitFirstOrLastAgg(true, null);
        }

        @Override
        public void visit(@NotNull final AggSpecFormula formula) {
            streamUnsupported("Formula");
            final GroupByChunkedOperator groupByChunkedOperator = new GroupByChunkedOperator(table, false,
                    resultPairs.stream().map(pair -> MatchPair.of((Pair) pair.input())).toArray(MatchPair[]::new));
            final FormulaChunkedOperator formulaChunkedOperator = new FormulaChunkedOperator(groupByChunkedOperator,
                    true, formula.formula(), formula.formulaParam(), MatchPair.fromPairs(resultPairs));
            addNoInputOperator(formulaChunkedOperator);
        }

        @Override
        public void visit(@NotNull final AggSpecLast last) {
            visitFirstOrLastAgg(false, null);
        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {
            visitMinOrMaxAgg(false);
        }

        @Override
        public void visit(@NotNull final AggSpecMedian median) {
            visitBasicAgg((t, n) -> getPercentileChunked(t, n, 0.50d, median.averageMedian()));
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            visitMinOrMaxAgg(true);
        }

        @Override
        public void visit(@NotNull final AggSpecPercentile pct) {
            visitBasicAgg((t, n) -> getPercentileChunked(t, n, pct.percentile(), pct.averageMedian()));
        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {
            visitSortedFirstOrLastAgg(sortedFirst.columns(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {
            visitSortedFirstOrLastAgg(sortedLast.columns(), false);
        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {
            visitBasicAgg((t, n) -> getVarChunked(t, n, true, false));
        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {
            visitBasicAgg(IterativeOperatorSpec::getSumChunked);
        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {
            visitBasicAgg((t, n) -> getUniqueChunked(t, n,
                    unique.includeNulls(), null, unique.nonUniqueSentinel(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecWAvg wAvg) {
            // TODO-RWC: Move this helper here or to a new landing place
            WeightedAverageSumAggregationFactory.getOperatorsAndInputs(table, wAvg.weight().name(), false,
                    MatchPair.fromPairs(resultPairs), operators, inputColumnNames, inputSources);
        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {
            WeightedAverageSumAggregationFactory.getOperatorsAndInputs(table, wSum.weight().name(), true,
                    MatchPair.fromPairs(resultPairs), operators, inputColumnNames, inputSources);
        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {
            visitBasicAgg((t, n) -> getVarChunked(t, n, false, false));
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Unsupported Operations
    // -----------------------------------------------------------------------------------------------------------------

    private interface UnsupportedRollupAggregations extends RollupAggregation.Visitor, AggSpec.Visitor {

        @Override
        @FinalDefault
        default void visit(@NotNull final FirstRowKey firstRowKey) {
            rollupUnsupported("FirstRowKey");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final LastRowKey lastRowKey) {
            rollupUnsupported("LastRowKey");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final AggSpecGroup group) {
            rollupUnsupported("Group");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final AggSpecFormula formula) {
            rollupUnsupported("Formula");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final AggSpecMedian median) {
            rollupUnsupported("Median");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final AggSpecPercentile pct) {
            rollupUnsupported("Percentile");
        }

        @Override
        @FinalDefault
        default void visit(@NotNull final AggSpecWAvg wAvg) {
            rollupUnsupported("WAvg");
        }
    }

    private static void rollupUnsupported(@NotNull final String operationName) {
        throw new UnsupportedOperationException(String.format("Agg%s is not supported for rollup()", operationName));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Base-level Aggregations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext} for rollup base aggregations.
     */
    private class RollupBaseConverter extends Converter
            implements RollupAggregation.Visitor, UnsupportedRollupAggregations {

        private boolean partitionFound;

        protected RollupBaseConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            super(table, groupByColumnNames);
        }

        @Override
        AggregationContext build() {
            if (!partitionFound) {
                transformers.add(new NoKeyLeafRollupAttributeSetter());
            }
            return super.build();
        }

        // -------------------------------------------------------------------------------------------------------------
        // RollupAggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final NullColumns nullColumns) {
            transformers.add(new NullColumnAggregationTransformer(nullColumns.resultColumns()));
        }

        @Override
        public void visit(@NotNull final Partition partition) {
            if (partition.includeConstituents()) {
                streamUnsupported("Partition for rollup with constituents included");
            }

            final QueryTable adjustedTable = partition.includeConstituents()
                    ? maybeCopyRlAttribute(table, table.updateView(ROLLUP_COLUMN + " = null"))
                    : table;
            final PartitionByChunkedOperator.AttributeCopier copier = partition.includeConstituents()
                    ? RollupAttributeCopier.LEAF_WITHCONSTITUENTS_INSTANCE
                    : RollupAttributeCopier.DEFAULT_INSTANCE;
            final PartitionByChunkedOperator partitionOperator = new PartitionByChunkedOperator(table,
                    adjustedTable, copier, Collections.emptyList(), groupByColumnNames);

            addNoInputOperator(partitionOperator);
            transformers.add(makeRollupKeysTransformer(groupByColumnNames));
            transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(partitionOperator, false,
                    partition.includeConstituents()));

            partitionFound = true;
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecLast last) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {
            // Supported
        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {
            // Supported
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Reaggregated Aggregations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext} for rollup reaggregated (not base level) aggregations.
     */
    private class RollupReaggregatedConverter extends Converter
            implements RollupAggregation.Visitor, UnsupportedRollupAggregations {

        protected RollupReaggregatedConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            super(table, groupByColumnNames);
        }

        // -------------------------------------------------------------------------------------------------------------
        // RollupAggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final NullColumns nullColumns) {
            transformers.add(new NullColumnAggregationTransformer(nullColumns.resultColumns()));
        }

        @Override
        public void visit(@NotNull final Partition partition) {
            if (partition.includeConstituents()) {
                throw new IllegalArgumentException("Cannot include constituents for reaggregated rollup levels");
            }

            final List<String> columnsToDrop = table.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                    .filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).collect(Collectors.toList());
            final QueryTable adjustedTable = columnsToDrop.isEmpty()
                    ? table
                    : maybeCopyRlAttribute(table, table.dropColumns(columnsToDrop));
            final PartitionByChunkedOperator partitionOperator = new PartitionByChunkedOperator(table,
                    adjustedTable, RollupAttributeCopier.DEFAULT_INSTANCE, Collections.emptyList(), groupByColumnNames);

            addNoInputOperator(partitionOperator);
            transformers.add(makeRollupKeysTransformer(groupByColumnNames));
            transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(partitionOperator, true, false));
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {

        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {

        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {

        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {

        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {

        }

        @Override
        public void visit(@NotNull final AggSpecLast last) {

        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {

        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {

        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {

        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {

        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {

        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {

        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {

        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {

        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {

        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private static ColumnSource<?> maybeReinterpretDateTimeAsLong(@NotNull final ColumnSource<?> inputSource) {
        return inputSource.getType() == DateTime.class
                ? ReinterpretUtils.dateTimeToLongSource(inputSource)
                : inputSource;
    }

    @NotNull
    private static String makeRedirectionName(IterativeIndexSpec inputAggregationStateFactory) {
        return IterativeIndexSpec.ROW_REDIRECTION_PREFIX + inputAggregationStateFactory.rollupColumnIdentifier
                + RollupConstants.ROLLUP_COLUMN_SUFFIX;
    }

    private static QueryTable maybeCopyRlAttribute(@NotNull final Table parent, @NotNull final Table child) {
        if (child != parent && parent.hasAttribute(REVERSE_LOOKUP_ATTRIBUTE)) {
            child.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, parent.getAttribute(REVERSE_LOOKUP_ATTRIBUTE));
        }
        return (QueryTable) child;
    }

    private static AggregationContextTransformer makeRollupKeysTransformer(@NotNull final String[] groupByColumnNames) {
        if (groupByColumnNames.length == 0) {
            return new StaticColumnSourceTransformer(RollupInfo.ROLLUP_COLUMN,
                    new SingleValueObjectColumnSource<>(SmartKey.EMPTY));
        }
        if (groupByColumnNames.length == 1) {
            return new RollupKeyColumnDuplicationTransformer(groupByColumnNames[0]);
        }
        return new RollupSmartKeyColumnDuplicationTransformer(groupByColumnNames);
    }

    private static class RollupTableMapAndReverseLookupAttributeSetter implements AggregationContextTransformer {

        private final PartitionByChunkedOperator partitionOperator;
        private final boolean reaggregated;
        private final boolean includeConstituents;
        private ReverseLookup reverseLookup;

        private RollupTableMapAndReverseLookupAttributeSetter(
                @NotNull final PartitionByChunkedOperator partitionOperator,
                final boolean reaggregated,
                final boolean includeConstituents) {
            this.partitionOperator = partitionOperator;
            this.reaggregated = reaggregated;
            this.includeConstituents = includeConstituents;
        }

        @Override
        public QueryTable transformResult(@NotNull final QueryTable table) {
            table.setAttribute(QueryTable.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, partitionOperator.getTableMap());
            if (reaggregated || includeConstituents) {
                table.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
            } else {
                setRollupLeafAttributes(table);
            }
            return table;
        }

        @Override
        public void setReverseLookupFunction(ToIntFunction<Object> reverseLookup) {
            this.reverseLookup = new ReverseLookupAdapter(reverseLookup);
        }

        private static class ReverseLookupAdapter implements ReverseLookup {

            private static final String[] KEY_COLUMN_NAMES = new String[] {ROLLUP_COLUMN};

            private final ToIntFunction<Object> reverseLookup;

            public ReverseLookupAdapter(@NotNull final ToIntFunction<Object> reverseLookup) {
                this.reverseLookup = reverseLookup;
            }

            @Override
            public long get(final Object key) {
                return reverseLookup.applyAsInt(key);
            }

            @Override
            public long getPrev(final Object key) {
                return get(key);
            }

            @Override
            public long getNoEntryValue() {
                return -1;
            }

            @Override
            public String[] getKeyColumns() {
                return KEY_COLUMN_NAMES;
            }
        }
    }

    private static class NoKeyLeafRollupAttributeSetter implements AggregationContextTransformer {
        @Override
        public QueryTable transformResult(@NotNull final QueryTable table) {
            setRollupLeafAttributes(table);
            return table;
        }
    }

    private static void setRollupLeafAttributes(@NotNull final QueryTable table) {
        table.setAttribute(Table.ROLLUP_LEAF_ATTRIBUTE, RollupInfo.LeafType.Normal);
        table.setAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, EmptyTableMap.INSTANCE);
        table.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    }
}
