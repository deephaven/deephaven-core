package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.engine.table.impl.by.IterativeOperatorSpec.*;

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
    public static AggregationContextFactory of(@NotNull final Collection<? extends Aggregation> aggregations) {
        return new AggregationProcessor(aggregations);
    }

    private final Collection<? extends Aggregation> aggregations;

    private AggregationProcessor(@NotNull final Collection<? extends Aggregation> aggregations) {
        this.aggregations = aggregations;
        final String duplicationErrorMessage = AggregationOutputs.of(aggregations).
                collect(Collectors.groupingBy(ColumnName::name, Collectors.counting())).
                entrySet().stream().filter(kv -> kv.getValue() > 1).
                map(kv -> kv.getKey() + " used " + kv.getValue() + " times").
                collect(Collectors.joining(", "));
        if (!duplicationErrorMessage.isBlank()) {
            throw new IllegalArgumentException("Duplicate output columns found: " + duplicationErrorMessage);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // AggregationContextFactory
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public AggregationContext makeAggregationContext(@NotNull Table table, @NotNull String... groupByColumnNames) {
        return new Converter(table, groupByColumnNames).build();
    }

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext}. Accumulates state by visiting each aggregation.
     */
    private class Converter implements Aggregation.Visitor, AggSpec.Visitor {

        private final Table table;
        private final String[] groupByColumnNames;

        private final boolean isAddOnly;
        private final boolean isStream;

        private final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
        private final List<String[]> inputColumnNames = new ArrayList<>();
        private final List<ChunkSource.WithPrev<Values>> inputSources = new ArrayList<>();
        private final List<AggregationContextTransformer> transformers = new ArrayList<>();

        private List<Pair> resultPairs = List.of();
        private int trackedFirstOrLastIndex = -1;
        private boolean partitionFound = false; // TODO-RWC: Add rollup support

        private Converter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            this.table = table;
            this.groupByColumnNames = groupByColumnNames;
            isAddOnly = ((BaseTable) table).isAddOnly();
            isStream = ((BaseTable) table).isStream();
        }

        private AggregationContext build() {
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

        // -------------------------------------------------------------------------------------------------------------
        // Aggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final Count count) {
            operators.add(new CountAggregationOperator(count.column().name()));
            inputSources.add(null);
            inputColumnNames.add(ZERO_LENGTH_STRING_ARRAY);
        }

        @Override
        public void visit(@NotNull final FirstRowKey firstRowKey) {
            visitFirstOrLastAgg(true, firstRowKey.column().name());
        }

        @Override
        public void visit(@NotNull final LastRowKey lastRowKey) {
            visitFirstOrLastAgg(false, lastRowKey.column().name());
        }

        @Override
        public void visit(@NotNull final ColumnAggregation columnAgg) {
            resultPairs = List.of(columnAgg.pair());
            columnAgg.spec().walk(this);
            resultPairs = List.of();
        }

        @Override
        public void visit(@NotNull final ColumnAggregations columnAggs) {
            resultPairs = columnAggs.pairs();
            columnAggs.spec().walk(this);
            resultPairs = List.of();
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
            operators.add(new GroupByChunkedOperator((QueryTable) table, true, MatchPair.fromPairs(resultPairs)));
            inputColumnNames.add(ZERO_LENGTH_STRING_ARRAY);
            inputSources.add(null);
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
            final GroupByChunkedOperator groupByChunkedOperator = new GroupByChunkedOperator((QueryTable) table, false,
                    resultPairs.stream().map(pair -> MatchPair.of((Pair) pair.input())).toArray(MatchPair[]::new));
            final FormulaChunkedOperator formulaChunkedOperator = new FormulaChunkedOperator(groupByChunkedOperator,
                    true, formula.formula(), formula.formulaParam(), MatchPair.fromPairs(resultPairs));
            operators.add(formulaChunkedOperator);
            inputColumnNames.add(ZERO_LENGTH_STRING_ARRAY);
            inputSources.add(null);
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
                    unique.includeNulls(), unique.onlyNullsValue(), unique.nonUniqueValue(), false, false));
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

        private void visitBasicAgg(BiFunction<Class<?>, String, IterativeChunkedAggregationOperator> operatorFactory) {
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

        private void visitMinOrMaxAgg(final boolean isMin) {
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

        private void visitFirstOrLastAgg(final boolean isFirst, final String exposeRedirectionAs) {
            if (exposeRedirectionAs != null) {
                streamUnsupported((isFirst ? "First" : "Last") + " with exposed row redirections (e.g. for rollup())");
            }
            final MatchPair[] resultMatchPairs = MatchPair.fromPairs(resultPairs);
            if (table.isRefreshing()) {
                if (isAddOnly) {
                    operators.add(new AddOnlyFirstOrLastChunkedOperator(isFirst, resultMatchPairs, table,
                            exposeRedirectionAs));
                } else if (isStream) {
                    operators.add(isFirst
                            ? new StreamFirstChunkedOperator(resultMatchPairs, table)
                            : new StreamLastChunkedOperator(resultMatchPairs, table));
                } else {
                    if (trackedFirstOrLastIndex >= 0) {
                        operators.add(((FirstOrLastChunkedOperator) operators.get(trackedFirstOrLastIndex))
                                .makeSecondaryOperator(isFirst, resultMatchPairs, table, exposeRedirectionAs));
                    } else {
                        trackedFirstOrLastIndex = operators.size();
                        operators.add(new FirstOrLastChunkedOperator(isFirst, resultMatchPairs, table,
                                exposeRedirectionAs));
                    }
                }
            } else {
                operators.add(new StaticFirstOrLastChunkedOperator(isFirst, resultMatchPairs, table,
                        exposeRedirectionAs));
            }
            inputColumnNames.add(ZERO_LENGTH_STRING_ARRAY);
            inputSources.add(null);
        }

        private void streamUnsupported(@NotNull final String operationName) {
            if (!isStream) {
                return;
            }
            throw new UnsupportedOperationException(String.format(
                    "Stream tables do not support Agg%s; use StreamTableTools.streamToAppendOnlyTable to accumulate full history",
                    operationName));
        }

        private void visitSortedFirstOrLastAgg(@NotNull final List<SortColumn> sortColumns, final boolean isFirst) {
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

        private void descendingSortedFirstOrLastUnsupported(@NotNull final SortColumn sortColumn,
                final boolean isFirst) {
            if (sortColumn.order() == SortColumn.Order.ASCENDING) {
                return;
            }
            throw new UnsupportedOperationException(String.format("%s does not support sort order in %s",
                    isFirst ? "SortedFirst" : "SortedLast", sortColumn));
        }
    }

    private static ColumnSource<?> maybeReinterpretDateTimeAsLong(@NotNull final ColumnSource<?> inputSource) {
        return inputSource.getType() == DateTime.class
                ? ReinterpretUtils.dateTimeToLongSource(inputSource)
                : inputSource;
    }
}
