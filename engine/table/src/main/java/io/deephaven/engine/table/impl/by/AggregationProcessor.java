package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.*;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.rollup.NullColumns;
import io.deephaven.engine.table.impl.by.rollup.Partition;
import io.deephaven.engine.table.impl.by.rollup.RollupAggregation;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.*;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.by.ssmpercentile.SsmChunkedPercentileOperator;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SingleValueObjectColumnSource;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.time.DateTime;
import io.deephaven.util.FunctionalInterfaces.TriFunction;
import io.deephaven.util.annotations.FinalDefault;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY;
import static io.deephaven.engine.table.ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY;
import static io.deephaven.engine.table.Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE;
import static io.deephaven.engine.table.Table.REVERSE_LOOKUP_ATTRIBUTE;
import static io.deephaven.engine.table.impl.RollupAttributeCopier.DEFAULT_INSTANCE;
import static io.deephaven.engine.table.impl.RollupAttributeCopier.LEAF_WITHCONSTITUENTS_INSTANCE;
import static io.deephaven.engine.table.impl.RollupInfo.ROLLUP_COLUMN;
import static io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY;
import static io.deephaven.engine.table.impl.by.RollupConstants.*;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.type.TypeUtils.*;

/**
 * Conversion tool to generate an {@link AggregationContextFactory} for a collection of {@link Aggregation
 * aggregations}.
 */
public class AggregationProcessor implements AggregationContextFactory {

    private enum Type {
        NORMAL, ROLLUP_BASE, ROLLUP_REAGGREGATED, SELECT_DISTINCT
    }

    private final Collection<? extends Aggregation> aggregations;
    private final Type type;

    /**
     * Convert a collection of {@link Aggregation aggregations} to an {@link AggregationContextFactory}.
     *
     * @param aggregations The {@link Aggregation aggregations}. Must not be further mutated by the caller. Will not be
     *        mutated by {@link AggregationProcessor}.
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory forAggregation(
            @NotNull final Collection<? extends Aggregation> aggregations) {
        return new AggregationProcessor(aggregations, Type.NORMAL);
    }

    /**
     * Convert a collection of {@link Aggregation aggregations} to an {@link AggregationContextFactory} for use in
     * computing the base level of a rollup.
     *
     * @param aggregations The {@link Aggregation aggregations}. Must not be further mutated by the caller. Will not be
     *        mutated by {@link AggregationProcessor}.
     * @param includeConstituents Whether constituents should be included via a partition aggregation
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory forRollupBase(
            @NotNull final Collection<? extends Aggregation> aggregations,
            final boolean includeConstituents) {
        // @formatter:off
        final Collection<? extends Aggregation> baseAggregations =
                Stream.concat(
                    aggregations.stream(),
                    Stream.of(includeConstituents ? Partition.of(true) : NullColumns.of(ROLLUP_COLUMN, Object.class))
                ).collect(Collectors.toList());
        // @formatter:on
        return new AggregationProcessor(baseAggregations, Type.ROLLUP_BASE);
    }

    /**
     * Convert a collection of {@link Aggregation aggregations} to an {@link AggregationContextFactory} for use in
     * computing a reaggregated level of a rollup.
     *
     * @param aggregations The {@link Aggregation aggregations}. Must not be further mutated by the caller. Will not be
     *        mutated by {@link AggregationProcessor}.
     * @param nullColumns Map of group-by column names and data types to aggregate with a null-column aggregation
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory forRollupReaggregated(
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Map<String, Class<?>> nullColumns) {
        // @formatter:off
        final Collection<? extends Aggregation> reaggregations =
                Stream.of(
                    Stream.of(NullColumns.from(nullColumns)),
                    aggregations.stream(),
                    Stream.of(Partition.of(false))
                ).flatMap(Function.identity()).collect(Collectors.toList());
        // @formatter:on
        return new AggregationProcessor(reaggregations, Type.ROLLUP_REAGGREGATED);
    }

    /**
     * Create a trivial {@link AggregationContextFactory} to implement {@link Table#selectDistinct select distinct}.
     * 
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory forSelectDistinct() {
        return new AggregationProcessor(Collections.emptyList(), Type.SELECT_DISTINCT);
    }

    private AggregationProcessor(
            @NotNull final Collection<? extends Aggregation> aggregations,
            @NotNull final Type type) {
        this.aggregations = aggregations;
        this.type = type;
        final String duplicationErrorMessage = AggregationPairs.outputsOf(aggregations)
                .collect(Collectors.groupingBy(ColumnName::name, Collectors.counting())).entrySet().stream()
                .filter(kv -> kv.getValue() > 1).map(kv -> kv.getKey() + " used " + kv.getValue() + " times")
                .collect(Collectors.joining(", "));
        if (!duplicationErrorMessage.isBlank()) {
            throw new IllegalArgumentException("Duplicate output columns found: " + duplicationErrorMessage);
        }
    }

    @Override
    public String toString() {
        return type.name() + ':' + aggregations;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // AggregationContextFactory
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
            @NotNull final String... groupByColumnNames) {
        switch (type) {
            case NORMAL:
                return new NormalConverter(table, groupByColumnNames).build();
            case ROLLUP_BASE:
                return new RollupBaseConverter(table, groupByColumnNames).build();
            case ROLLUP_REAGGREGATED:
                return new RollupReaggregatedConverter(table, groupByColumnNames).build();
            case SELECT_DISTINCT:
                return makeEmptyAggregationContext();
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
        // Partial AggSpec.Visitor (for cases common to all types)
        // -------------------------------------------------------------------------------------------------------------

        // THIS SPACE INTENTIONALLY LEFT BLANK

        // -------------------------------------------------------------------------------------------------------------
        // Helpers for visitors
        // -------------------------------------------------------------------------------------------------------------

        void addNoInputOperator(@NotNull final IterativeChunkedAggregationOperator operator) {
            addOperator(operator, null, ZERO_LENGTH_STRING_ARRAY);
        }

        @SafeVarargs
        final void addOperator(@NotNull final IterativeChunkedAggregationOperator operator,
                @Nullable final ChunkSource.WithPrev<Values> inputSource,
                @NotNull final Stream<String>... inputColumnNames) {
            addOperator(operator, inputSource,
                    Stream.of(inputColumnNames).flatMap(Function.identity()).toArray(String[]::new));
        }

        final void addOperator(@NotNull final IterativeChunkedAggregationOperator operator,
                @Nullable final ChunkSource.WithPrev<Values> inputSource,
                @NotNull final String... inputColumnNames) {
            operators.add(operator);
            this.inputColumnNames.add(inputColumnNames);
            inputSources.add(inputSource);
        }

        final void addBasicOperators(
                BiFunction<Class<?>, String, IterativeChunkedAggregationOperator> operatorFactory) {
            for (final Pair pair : resultPairs) {
                final String inputName = pair.input().name();
                final String resultName = pair.output().name();
                final ColumnSource<?> rawInputSource = table.getColumnSource(inputName);
                final Class<?> type = rawInputSource.getType();
                final ColumnSource<?> inputSource = maybeReinterpretDateTimeAsLong(rawInputSource);

                addOperator(operatorFactory.apply(type, resultName), inputSource, inputName);
            }
        }

        final void addMinOrMaxOperators(final boolean isMin) {
            for (final Pair pair : resultPairs) {
                final String inputName = pair.input().name();
                final String resultName = pair.output().name();

                addMinOrMaxOperator(isMin, inputName, resultName);
            }
        }

        final void addMinOrMaxOperator(final boolean isMin, @NotNull final String inputName,
                @NotNull final String resultName) {
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
                                addOperator(
                                        ssmChunkedMinMaxOperator.makeSecondaryOperator(isMin, resultName),
                                        null, inputName);
                            },
                            () -> addOperator(
                                    makeMinOrMaxOperator(type, resultName, isMin, isAddOnly || isStream),
                                    inputSource, inputName));
        }

        final void addFirstOrLastOperators(final boolean isFirst, final String exposeRedirectionAs) {
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

        final void addSortedFirstOrLastOperator(@NotNull final List<SortColumn> sortColumns, final boolean isFirst) {
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
            addOperator(
                    makeSortedFirstOrLastOperator(inputSource.getChunkType(), isFirst, aggregations.size() > 1,
                            MatchPair.fromPairs(resultPairs), table),
                    inputSource, sortColumnNames);
        }

        final void descendingSortedFirstOrLastUnsupported(@NotNull final SortColumn sortColumn, final boolean isFirst) {
            if (sortColumn.order() == SortColumn.Order.ASCENDING) {
                return;
            }
            throw new UnsupportedOperationException(String.format("%s does not support sort order in %s",
                    isFirst ? "SortedFirst" : "SortedLast", sortColumn));
        }

        final void addWeightedAvgOrSumOperator(@NotNull final String weightName, final boolean isSum) {
            final ColumnSource<?> weightSource = table.getColumnSource(weightName);
            final boolean weightSourceIsFloatingPoint;
            if (isInteger(weightSource.getChunkType())) {
                weightSourceIsFloatingPoint = false;
            } else if (isFloatingPoint(weightSource.getChunkType())) {
                weightSourceIsFloatingPoint = true;
            } else {
                throw new UnsupportedOperationException(
                        String.format("Invalid type %s in weight column %s for AggW%s",
                                weightSource.getType(), weightName, isSum ? "Sum" : "Avg"));
            }

            final MutableBoolean anyIntegerResults = new MutableBoolean();
            final MutableBoolean anyFloatingPointResults = new MutableBoolean();
            final List<Result> results = resultPairs.stream().map(pair -> {
                final ColumnSource<?> inputSource = table.getColumnSource(pair.input().name());
                final ResultType resultType;
                if (isInteger(inputSource.getChunkType())) {
                    if (!weightSourceIsFloatingPoint && isSum) {
                        anyIntegerResults.setTrue();
                        resultType = ResultType.INTEGER;
                    } else {
                        anyFloatingPointResults.setTrue();
                        resultType = ResultType.FLOATING_POINT;
                    }
                } else if (isFloatingPoint(inputSource.getChunkType())) {
                    anyFloatingPointResults.setTrue();
                    resultType = ResultType.FLOATING_POINT;
                } else {
                    throw new UnsupportedOperationException(
                            String.format("Invalid type %s in column %s for AggW%s weighted by %s",
                                    inputSource.getType(), pair.input().name(), isSum ? "Sum" : "Avg", weightName));
                }
                return new Result(pair, resultType, inputSource);
            }).collect(Collectors.toList());

            final LongWeightRecordingInternalOperator longWeightOperator;
            if (anyIntegerResults.booleanValue()) {
                longWeightOperator = new LongWeightRecordingInternalOperator(weightSource.getChunkType());
                addOperator(longWeightOperator, weightSource, Stream.of(weightName),
                        results.stream().filter(r -> r.type == ResultType.INTEGER).map(r -> r.pair.input().name()));
            } else {
                longWeightOperator = null;
            }

            final DoubleWeightRecordingInternalOperator doubleWeightOperator;
            if (anyFloatingPointResults.booleanValue()) {
                doubleWeightOperator = new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
                addOperator(doubleWeightOperator, weightSource, Stream.of(weightName),
                        results.stream().filter(r -> r.type == ResultType.FLOATING_POINT)
                                .map(r -> r.pair.input().name()));
            } else {
                doubleWeightOperator = null;
            }

            results.forEach(r -> {
                final IterativeChunkedAggregationOperator resultOperator;
                if (isSum) {
                    if (r.type == ResultType.INTEGER) {
                        resultOperator = new LongChunkedWeightedSumOperator(
                                r.source.getChunkType(), longWeightOperator, r.pair.output().name());
                    } else {
                        resultOperator = new DoubleChunkedWeightedSumOperator(
                                r.source.getChunkType(), doubleWeightOperator, r.pair.output().name());
                    }
                } else {
                    resultOperator = new ChunkedWeightedAverageOperator(
                            r.source.getChunkType(), doubleWeightOperator, r.pair.output().name());
                }
                addOperator(resultOperator, r.source, r.pair.input().name(), weightName);
            });
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Standard Aggregations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext} for standard aggregations. Accumulates state by visiting each aggregation.
     */
    private final class NormalConverter extends Converter {

        private NormalConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            super(table, groupByColumnNames);
        }

        // -------------------------------------------------------------------------------------------------------------
        // Aggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final Count count) {
            addNoInputOperator(new CountAggregationOperator(count.column().name()));
        }

        @Override
        public void visit(@NotNull final FirstRowKey firstRowKey) {
            addFirstOrLastOperators(true, firstRowKey.column().name());
        }

        @Override
        public void visit(@NotNull final LastRowKey lastRowKey) {
            addFirstOrLastOperators(false, lastRowKey.column().name());
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, true));
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            addBasicOperators((t, n) -> makeCountDistinctOperator(t, n, countDistinct.countNulls(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            addBasicOperators((t, n) -> makeDistinctOperator(t, n, distinct.includeNulls(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecGroup group) {
            streamUnsupported("Group");
            addNoInputOperator(new GroupByChunkedOperator(table, true, MatchPair.fromPairs(resultPairs)));
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            addBasicOperators((t, n) -> makeAvgOperator(t, n, false));
        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {
            addFirstOrLastOperators(true, null);
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
            addFirstOrLastOperators(false, null);
        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {
            addMinOrMaxOperators(false);
        }

        @Override
        public void visit(@NotNull final AggSpecMedian median) {
            addBasicOperators((t, n) -> new SsmChunkedPercentileOperator(t, 0.50d, median.averageMedian(), n));
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            addMinOrMaxOperators(true);
        }

        @Override
        public void visit(@NotNull final AggSpecPercentile pct) {
            addBasicOperators((t, n) -> new SsmChunkedPercentileOperator(t, pct.percentile(), pct.averageMedian(), n));
        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {
            addSortedFirstOrLastOperator(sortedFirst.columns(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {
            addSortedFirstOrLastOperator(sortedLast.columns(), false);
        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {
            addBasicOperators((t, n) -> makeVarOrStdOperator(t, n, true, false));
        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, false));
        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {
            addBasicOperators((t, n) -> makeUniqueOperator(t, n,
                    unique.includeNulls(), null, unique.nonUniqueSentinel(), false, false));
        }

        @Override
        public void visit(@NotNull final AggSpecWAvg wAvg) {
            addWeightedAvgOrSumOperator(wAvg.weight().name(), false);
        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {
            addWeightedAvgOrSumOperator(wSum.weight().name(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {
            addBasicOperators((t, n) -> makeVarOrStdOperator(t, n, false, false));
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Unsupported Operations
    // -----------------------------------------------------------------------------------------------------------------

    private interface UnsupportedRollupAggregations extends RollupAggregation.Visitor, AggSpec.Visitor {

        // -------------------------------------------------------------------------------------------------------------
        // RollupAggregation.Visitor for unsupported aggregations
        // -------------------------------------------------------------------------------------------------------------

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

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor for unsupported column aggregation specs
        // -------------------------------------------------------------------------------------------------------------

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
    private final class RollupBaseConverter extends Converter
            implements RollupAggregation.Visitor, UnsupportedRollupAggregations {

        private boolean partitionFound;
        private int nextColumnIdentifier = 0;

        private RollupBaseConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
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
        public void visit(@NotNull final Count count) {
            addNoInputOperator(new CountAggregationOperator(count.column().name()));
        }

        @Override
        public void visit(@NotNull final NullColumns nullColumns) {
            transformers.add(new NullColumnAggregationTransformer(nullColumns.resultColumns()));
        }

        @Override
        public void visit(@NotNull final Partition partition) {
            if (!partition.includeConstituents()) {
                throw new IllegalArgumentException(
                        "Partition isn't used for rollup base levels unless constituents are included");
            }
            streamUnsupported("Partition for rollup with constituents included");

            final QueryTable adjustedTable = maybeCopyRlAttribute(table, table.updateView(ROLLUP_COLUMN + " = null"));
            final PartitionByChunkedOperator partitionOperator = new PartitionByChunkedOperator(table,
                    adjustedTable, LEAF_WITHCONSTITUENTS_INSTANCE, Collections.emptyList(), groupByColumnNames);

            addNoInputOperator(partitionOperator);
            transformers.add(makeRollupKeysTransformer(groupByColumnNames));
            transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(partitionOperator, false, true));

            partitionFound = true;
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, true));
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            addBasicOperators((t, n) -> makeCountDistinctOperator(t, n, countDistinct.countNulls(), true, false));
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            addBasicOperators((t, n) -> makeDistinctOperator(t, n, distinct.includeNulls(), true, false));
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            addBasicOperators((t, n) -> makeAvgOperator(t, n, true));
        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {
            addFirstOrLastOperators(true, makeRedirectionName(nextColumnIdentifier++));
        }

        @Override
        public void visit(@NotNull final AggSpecLast last) {
            addFirstOrLastOperators(false, makeRedirectionName(nextColumnIdentifier++));
        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {
            addMinOrMaxOperators(false);
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            addMinOrMaxOperators(true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {
            addSortedFirstOrLastOperator(sortedFirst.columns(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {
            addSortedFirstOrLastOperator(sortedLast.columns(), false);
        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {
            addBasicOperators((t, n) -> makeVarOrStdOperator(t, n, true, true));
        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, false));
        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {
            addBasicOperators((t, n) -> makeUniqueOperator(t, n,
                    unique.includeNulls(), null, unique.nonUniqueSentinel(), true, false));
        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {
            addWeightedAvgOrSumOperator(wSum.weight().name(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {
            addBasicOperators((t, n) -> makeVarOrStdOperator(t, n, false, true));
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Reaggregated Aggregations
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Implementation class for conversion from a collection of {@link Aggregation aggregations} to an
     * {@link AggregationContext} for rollup reaggregated (not base level) aggregations.
     */
    private final class RollupReaggregatedConverter extends Converter
            implements RollupAggregation.Visitor, UnsupportedRollupAggregations {

        private int nextColumnIdentifier = 0;

        private RollupReaggregatedConverter(@NotNull final Table table, @NotNull final String... groupByColumnNames) {
            super(table, groupByColumnNames);
        }

        // -------------------------------------------------------------------------------------------------------------
        // RollupAggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final Count count) {
            addNoInputOperator(new LongChunkedSumOperator(false, count.column().name()));
        }

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
                    adjustedTable, DEFAULT_INSTANCE, Collections.emptyList(), groupByColumnNames);

            addNoInputOperator(partitionOperator);
            transformers.add(makeRollupKeysTransformer(groupByColumnNames));
            transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(partitionOperator, true, false));
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            reaggregateAsSum();
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            reaggregateSsmBackedOperator((ssmType, priorResultType, n) -> makeCountDistinctOperator(ssmType, n,
                    countDistinct.countNulls(), true, true));
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            reaggregateSsmBackedOperator((ssmType, priorResultType, n) -> makeDistinctOperator(priorResultType, n,
                    distinct.includeNulls(), true, true));
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            reaggregateAvgOperator();
        }

        @Override
        public void visit(@NotNull final AggSpecFirst first) {
            reaggregateFirstOrLastOperator(true);
        }

        @Override
        public void visit(@NotNull final AggSpecLast last) {
            reaggregateFirstOrLastOperator(false);
        }

        @Override
        public void visit(@NotNull final AggSpecMax max) {
            reaggregateMinOrMaxOperators(false);
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            reaggregateMinOrMaxOperators(true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedFirst sortedFirst) {
            reaggregateSortedFirstOrLastOperator(sortedFirst.columns(), true);
        }

        @Override
        public void visit(@NotNull final AggSpecSortedLast sortedLast) {
            reaggregateSortedFirstOrLastOperator(sortedLast.columns(), false);
        }

        @Override
        public void visit(@NotNull final AggSpecStd std) {
            reaggregateStdOrVarOperators(true);
        }

        @Override
        public void visit(@NotNull final AggSpecSum sum) {
            reaggregateAsSum();
        }

        @Override
        public void visit(@NotNull final AggSpecUnique unique) {
            reaggregateSsmBackedOperator((ssmType, priorResultType, n) -> makeUniqueOperator(
                    priorResultType, n, unique.includeNulls(), null, unique.nonUniqueSentinel(), true, true));
        }

        @Override
        public void visit(@NotNull final AggSpecWSum wSum) {
            reaggregateAsSum();
        }

        @Override
        public void visit(@NotNull final AggSpecVar var) {
            reaggregateStdOrVarOperators(false);
        }

        private void reaggregateAsSum() {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();
                final ColumnSource<?> resultSource = table.getColumnSource(resultName);

                addOperator(makeSumOperator(resultSource.getType(), resultName, false), resultSource, resultName);
            }
        }

        private void reaggregateSsmBackedOperator(
                TriFunction<Class<?>, Class<?>, String, IterativeChunkedAggregationOperator> operatorFactory) {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();
                final String ssmName = resultName + ROLLUP_DISTINCT_SSM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final ColumnSource<SegmentedSortedMultiSet<?>> ssmSource = table.getColumnSource(ssmName);
                final ColumnSource<?> priorResultSource = table.getColumnSource(resultName);
                final IterativeChunkedAggregationOperator operator = operatorFactory.apply(
                        ssmSource.getComponentType(), priorResultSource.getComponentType(), resultName);

                addOperator(operator, ssmSource, ssmName);
            }
        }

        private void reaggregateFirstOrLastOperator(final boolean isFirst) {
            final ColumnName redirectionColumnName = ColumnName.of(makeRedirectionName(nextColumnIdentifier++));
            resultPairs = Stream.concat(
                    resultPairs.stream().map(Pair::output),
                    Stream.of(redirectionColumnName))
                    .collect(Collectors.toList());
            addSortedFirstOrLastOperator(List.of(SortColumn.asc(redirectionColumnName)), isFirst);
        }

        private void reaggregateSortedFirstOrLastOperator(
                @NotNull final List<SortColumn> sortColumns, final boolean isFirst) {
            resultPairs = resultPairs.stream().map(Pair::output).collect(Collectors.toList());
            addSortedFirstOrLastOperator(sortColumns, isFirst);
        }

        private void reaggregateMinOrMaxOperators(final boolean isMin) {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();
                addMinOrMaxOperator(isMin, resultName, resultName);
            }
        }

        private void reaggregateAvgOperator() {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();

                final String runningSumName = resultName + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final Class<?> runningSumType = table.getColumnSource(runningSumName).getType();

                final String nonNullCountName = resultName + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final LongChunkedSumOperator nonNullCountOp = addAndGetLongSumOperator(nonNullCountName);

                if (runningSumType == double.class) {
                    final DoubleChunkedSumOperator runningSumOp = addAndGetDoubleSumOperator(runningSumName);

                    final String nanCountName = resultName + ROLLUP_NAN_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator nanCountOp = addAndGetLongSumOperator(nanCountName);

                    final String piCountName = resultName + ROLLUP_PI_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator piCountOp = addAndGetLongSumOperator(piCountName);

                    final String niCountName = resultName + ROLLUP_NI_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator niCountOp = addAndGetLongSumOperator(niCountName);

                    final Class<?> resultType = table.getColumnSource(resultName).getType();
                    if (resultType == float.class) {
                        addOperator(new FloatChunkedReAvgOperator(resultName,
                                runningSumOp, nonNullCountOp, nanCountOp, piCountOp, niCountOp),
                                null, nonNullCountName, runningSumName, nanCountName, piCountName, niCountName);
                    } else { // resultType == double.class
                        addOperator(new DoubleChunkedReAvgOperator(resultName,
                                runningSumOp, nonNullCountOp, nanCountOp, piCountOp, niCountOp),
                                null, nonNullCountName, runningSumName, nanCountName, piCountName, niCountName);
                    }
                } else if (BigInteger.class.isAssignableFrom(runningSumType)) {
                    final BigIntegerChunkedSumOperator runningSumOp = addAndGetBigIntegerSumOperator(runningSumName);
                    addOperator(new BigIntegerChunkedReAvgOperator(resultName, runningSumOp, nonNullCountOp),
                            null, nonNullCountName, runningSumName);
                } else if (BigDecimal.class.isAssignableFrom(runningSumType)) {
                    final BigDecimalChunkedSumOperator runningSumOp = addAndGetBigDecimalSumOperator(runningSumName);
                    addOperator(new BigDecimalChunkedReAvgOperator(resultName, runningSumOp, nonNullCountOp),
                            null, nonNullCountName, runningSumName);
                } else {
                    final LongChunkedSumOperator runningSumOp = addAndGetLongSumOperator(runningSumName);
                    addOperator(new IntegralChunkedReAvgOperator(resultName, runningSumOp, nonNullCountOp),
                            null, nonNullCountName, runningSumName);
                }
            }
        }

        private void reaggregateStdOrVarOperators(final boolean isStd) {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();

                final String runningSumName = resultName + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final Class<?> runningSumType = table.getColumnSource(runningSumName).getType();

                final String runningSum2Name = resultName + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;

                final String nonNullCountName = resultName + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final LongChunkedSumOperator nonNullCountOp = addAndGetLongSumOperator(nonNullCountName);

                if (runningSumType == double.class) {
                    final DoubleChunkedSumOperator runningSumOp = addAndGetDoubleSumOperator(runningSumName);
                    final DoubleChunkedSumOperator runningSum2Op = addAndGetDoubleSumOperator(runningSum2Name);

                    final String nanCountName = resultName + ROLLUP_NAN_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator nanCountOp = addAndGetLongSumOperator(nanCountName);

                    final String piCountName = resultName + ROLLUP_PI_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator piCountOp = addAndGetLongSumOperator(piCountName);

                    final String niCountName = resultName + ROLLUP_NI_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                    final LongChunkedSumOperator niCountOp = addAndGetLongSumOperator(niCountName);

                    addOperator(new FloatChunkedReVarOperator(resultName, isStd, runningSumOp, runningSum2Op,
                            nonNullCountOp, nanCountOp, piCountOp, niCountOp), null,
                            nonNullCountName, runningSumName, runningSum2Name, nanCountName, piCountName, niCountName);
                } else if (BigInteger.class.isAssignableFrom(runningSumType)) {
                    final BigIntegerChunkedSumOperator runningSumOp = addAndGetBigIntegerSumOperator(runningSumName);
                    final BigIntegerChunkedSumOperator runningSum2Op = addAndGetBigIntegerSumOperator(runningSum2Name);
                    addOperator(new BigIntegerChunkedReVarOperator(resultName, isStd,
                            runningSumOp, runningSum2Op, nonNullCountOp),
                            null, nonNullCountName, runningSumName, runningSum2Name);
                } else if (BigDecimal.class.isAssignableFrom(runningSumType)) {
                    final BigDecimalChunkedSumOperator runningSumOp = addAndGetBigDecimalSumOperator(runningSumName);
                    final BigDecimalChunkedSumOperator runningSum2Op = addAndGetBigDecimalSumOperator(runningSum2Name);
                    addOperator(new BigDecimalChunkedReVarOperator(resultName, isStd,
                            runningSumOp, runningSum2Op, nonNullCountOp),
                            null, nonNullCountName, runningSumName, runningSum2Name);
                } else {
                    final DoubleChunkedSumOperator runningSumOp = addAndGetDoubleSumOperator(runningSumName);
                    final DoubleChunkedSumOperator runningSum2Op = addAndGetDoubleSumOperator(runningSum2Name);
                    addOperator(new IntegralChunkedReVarOperator(resultName, isStd,
                            runningSumOp, runningSum2Op, nonNullCountOp),
                            null, nonNullCountName, runningSumName, runningSum2Name);
                }
            }
        }

        private BigDecimalChunkedSumOperator addAndGetBigDecimalSumOperator(@NotNull final String inputColumnName) {
            return getAndAddBasicOperator(n -> new BigDecimalChunkedSumOperator(false, n), inputColumnName);
        }

        private BigIntegerChunkedSumOperator addAndGetBigIntegerSumOperator(@NotNull final String inputColumnName) {
            return getAndAddBasicOperator(n -> new BigIntegerChunkedSumOperator(false, n), inputColumnName);
        }

        private DoubleChunkedSumOperator addAndGetDoubleSumOperator(@NotNull final String inputColumnName) {
            return getAndAddBasicOperator(n -> new DoubleChunkedSumOperator(false, n), inputColumnName);
        }

        private LongChunkedSumOperator addAndGetLongSumOperator(@NotNull final String inputColumnName) {
            return getAndAddBasicOperator(n -> new LongChunkedSumOperator(false, n), inputColumnName);
        }

        private <OP_TYPE extends IterativeChunkedAggregationOperator> OP_TYPE getAndAddBasicOperator(
                @NotNull final Function<String, OP_TYPE> opFactory, @NotNull final String inputColumnName) {
            OP_TYPE operator = opFactory.apply(inputColumnName);
            addOperator(operator, table.getColumnSource(inputColumnName), inputColumnName);
            return operator;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Basic Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private static AggregationContext makeEmptyAggregationContext() {
        // noinspection unchecked
        return new AggregationContext(
                ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY,
                ZERO_LENGTH_STRING_ARRAY_ARRAY,
                ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY);
    }

    private static ColumnSource<?> maybeReinterpretDateTimeAsLong(@NotNull final ColumnSource<?> inputSource) {
        return inputSource.getType() == DateTime.class
                ? ReinterpretUtils.dateTimeToLongSource(inputSource)
                : inputSource;
    }

    private static boolean isFloatingPoint(@NotNull final ChunkType chunkType) {
        return chunkType == ChunkType.Float
                || chunkType == ChunkType.Double;
    }

    private static boolean isInteger(@NotNull final ChunkType chunkType) {
        return chunkType == ChunkType.Char
                || chunkType == ChunkType.Byte
                || chunkType == ChunkType.Short
                || chunkType == ChunkType.Int
                || chunkType == ChunkType.Long;
    }

    private enum ResultType {
        INTEGER, FLOATING_POINT
    }

    private static class Result {

        private final Pair pair;
        private final ResultType type;
        private final ColumnSource<?> source;

        private Result(@NotNull final Pair pair, @NotNull final ResultType type,
                @NotNull final ColumnSource<?> source) {
            this.pair = pair;
            this.type = type;
            this.source = source;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Operator Construction Helpers (e.g. to multiplex on input/output data type)
    // -----------------------------------------------------------------------------------------------------------------

    private static IterativeChunkedAggregationOperator makeSumOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean isAbsolute) {
        if (type == Boolean.class || type == boolean.class) {
            return new BooleanChunkedSumOperator(name);
        } else if (type == Byte.class || type == byte.class) {
            return new ByteChunkedSumOperator(isAbsolute, name);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedSumOperator(isAbsolute, name);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedSumOperator(isAbsolute, name);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedSumOperator(isAbsolute, name);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedSumOperator(isAbsolute, name);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedSumOperator(isAbsolute, name);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedSumOperator(isAbsolute, name);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedSumOperator(isAbsolute, name);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedSumOperator(isAbsolute, name);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    private static IterativeChunkedAggregationOperator makeMinOrMaxOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean isMin,
            final boolean isStreamOrAddOnly) {
        if (!isStreamOrAddOnly) {
            return new SsmChunkedMinMaxOperator(type, isMin, name);
        }
        if (type == Byte.class || type == byte.class) {
            return new ByteChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            return new LongChunkedAddOnlyMinMaxOperator(type, isMin, name);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedAddOnlyMinMaxOperator(isMin, name);
        } else if (type == Boolean.class || type == boolean.class) {
            return new BooleanChunkedAddOnlyMinMaxOperator(isMin, name);
        } else {
            return new ObjectChunkedAddOnlyMinMaxOperator(type, isMin, name);
        }
    }

    private static IterativeChunkedAggregationOperator makeCountDistinctOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean countNulls,
            final boolean exposeInternal,
            final boolean reaggregated) {
        if (type == Byte.class || type == byte.class) {
            return reaggregated
                    ? new ByteRollupCountDistinctOperator(name, countNulls)
                    : new ByteChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return reaggregated
                    ? new CharRollupCountDistinctOperator(name, countNulls)
                    : new CharChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return reaggregated
                    ? new DoubleRollupCountDistinctOperator(name, countNulls)
                    : new DoubleChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return reaggregated
                    ? new FloatRollupCountDistinctOperator(name, countNulls)
                    : new FloatChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return reaggregated
                    ? new IntRollupCountDistinctOperator(name, countNulls)
                    : new IntChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            return reaggregated
                    ? new LongRollupCountDistinctOperator(name, countNulls)
                    : new LongChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return reaggregated
                    ? new ShortRollupCountDistinctOperator(name, countNulls)
                    : new ShortChunkedCountDistinctOperator(name, countNulls, exposeInternal);
        } else {
            return reaggregated
                    ? new ObjectRollupCountDistinctOperator(type, name, countNulls)
                    : new ObjectChunkedCountDistinctOperator(type, name, countNulls, exposeInternal);
        }
    }

    private static IterativeChunkedAggregationOperator makeDistinctOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean includeNulls,
            final boolean exposeInternal,
            final boolean reaggregated) {
        if (type == Byte.class || type == byte.class) {
            return reaggregated
                    ? new ByteRollupDistinctOperator(name, includeNulls)
                    : new ByteChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return reaggregated
                    ? new CharRollupDistinctOperator(name, includeNulls)
                    : new CharChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return reaggregated
                    ? new DoubleRollupDistinctOperator(name, includeNulls)
                    : new DoubleChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return reaggregated
                    ? new FloatRollupDistinctOperator(name, includeNulls)
                    : new FloatChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return reaggregated
                    ? new IntRollupDistinctOperator(name, includeNulls)
                    : new IntChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            return reaggregated
                    ? new LongRollupDistinctOperator(type, name, includeNulls)
                    : new LongChunkedDistinctOperator(type, name, includeNulls, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return reaggregated
                    ? new ShortRollupDistinctOperator(name, includeNulls)
                    : new ShortChunkedDistinctOperator(name, includeNulls, exposeInternal);
        } else {
            return reaggregated
                    ? new ObjectRollupDistinctOperator(type, name, includeNulls)
                    : new ObjectChunkedDistinctOperator(type, name, includeNulls, exposeInternal);
        }
    }

    private static IterativeChunkedAggregationOperator makeUniqueOperator(
            @NotNull final Class<?> type,
            @NotNull final String resultName,
            final boolean includeNulls,
            @SuppressWarnings("SameParameterValue") final Object onlyNullsSentinel,
            final Object nonUniqueSentinel,
            final boolean exposeInternal,
            final boolean reaggregated) {
        checkType(resultName, "Only Nulls Sentinel", type, onlyNullsSentinel);
        checkType(resultName, "Non Unique Sentinel", type, nonUniqueSentinel);

        if (type == Byte.class || type == byte.class) {
            final byte onsAsType = (onlyNullsSentinel == null) ? NULL_BYTE : ((Number) onlyNullsSentinel).byteValue();
            final byte nusAsType = (nonUniqueSentinel == null) ? NULL_BYTE : ((Number) nonUniqueSentinel).byteValue();
            return reaggregated
                    ? new ByteRollupUniqueOperator(resultName, includeNulls, onsAsType, nusAsType)
                    : new ByteChunkedUniqueOperator(resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Character.class || type == char.class) {
            return reaggregated
                    ? new CharRollupUniqueOperator(resultName, includeNulls,
                            unbox((Character) onlyNullsSentinel),
                            unbox((Character) nonUniqueSentinel))
                    : new CharChunkedUniqueOperator(resultName, includeNulls, exposeInternal,
                            unbox((Character) onlyNullsSentinel),
                            unbox((Character) nonUniqueSentinel));
        } else if (type == Double.class || type == double.class) {
            final double onsAsType =
                    (onlyNullsSentinel == null) ? NULL_DOUBLE : ((Number) onlyNullsSentinel).doubleValue();
            final double nusAsType =
                    (nonUniqueSentinel == null) ? NULL_DOUBLE : ((Number) nonUniqueSentinel).doubleValue();
            return reaggregated
                    ? new DoubleRollupUniqueOperator(resultName, includeNulls, onsAsType, nusAsType)
                    : new DoubleChunkedUniqueOperator(resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Float.class || type == float.class) {
            final float onsAsType =
                    (onlyNullsSentinel == null) ? NULL_FLOAT : ((Number) onlyNullsSentinel).floatValue();
            final float nusAsType =
                    (nonUniqueSentinel == null) ? NULL_FLOAT : ((Number) nonUniqueSentinel).floatValue();
            return reaggregated
                    ? new FloatRollupUniqueOperator(resultName, includeNulls, onsAsType, nusAsType)
                    : new FloatChunkedUniqueOperator(resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Integer.class || type == int.class) {
            final int onsAsType = (onlyNullsSentinel == null) ? NULL_INT : ((Number) onlyNullsSentinel).intValue();
            final int nusAsType = (nonUniqueSentinel == null) ? NULL_INT : ((Number) nonUniqueSentinel).intValue();
            return reaggregated
                    ? new IntRollupUniqueOperator(resultName, includeNulls, onsAsType, nusAsType)
                    : new IntChunkedUniqueOperator(resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
        } else if (type == Long.class || type == long.class || type == DateTime.class) {
            final long onsAsType;
            final long nusAsType;
            if (type == DateTime.class) {
                onsAsType = (onlyNullsSentinel == null) ? NULL_LONG : ((DateTime) onlyNullsSentinel).getNanos();
                nusAsType = (nonUniqueSentinel == null) ? NULL_LONG : ((DateTime) nonUniqueSentinel).getNanos();
            } else {
                onsAsType = (onlyNullsSentinel == null) ? NULL_LONG : ((Number) onlyNullsSentinel).longValue();
                nusAsType = (nonUniqueSentinel == null) ? NULL_LONG : ((Number) nonUniqueSentinel).longValue();
            }
            return reaggregated
                    ? new LongRollupUniqueOperator(type, resultName, includeNulls, onsAsType, nusAsType)
                    : new LongChunkedUniqueOperator(type, resultName, includeNulls, exposeInternal, onsAsType,
                            nusAsType);
        } else if (type == Short.class || type == short.class) {
            final short onsAsType =
                    (onlyNullsSentinel == null) ? NULL_SHORT : ((Number) onlyNullsSentinel).shortValue();
            final short nusAsType =
                    (nonUniqueSentinel == null) ? NULL_SHORT : ((Number) nonUniqueSentinel).shortValue();
            return reaggregated
                    ? new ShortRollupUniqueOperator(resultName, includeNulls, onsAsType, nusAsType)
                    : new ShortChunkedUniqueOperator(resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
        } else {
            return reaggregated
                    ? new ObjectRollupUniqueOperator(type, resultName, includeNulls, onlyNullsSentinel,
                            nonUniqueSentinel)
                    : new ObjectChunkedUniqueOperator(type, resultName, includeNulls, exposeInternal, onlyNullsSentinel,
                            nonUniqueSentinel);
        }
    }

    private static void checkType(@NotNull final String name, @NotNull final String valueIntent,
            @NotNull Class<?> expected, final Object value) {
        expected = getBoxedType(expected);
        if (value != null && !expected.isAssignableFrom(value.getClass())) {
            if (isNumeric(expected) && isNumeric(value.getClass())) {
                if (checkNumericCompatibility((Number) value, expected)) {
                    return;
                }
                throw new IllegalArgumentException(
                        String.format("For result column %s the %s %s is larger than can be represented with a %s",
                                name, valueIntent, value, expected.getName()));
            }
            throw new IllegalArgumentException(
                    String.format("For result column %s the %s must be of type %s but is %s",
                            name, valueIntent, expected.getName(), value.getClass().getName()));
        }
    }

    private static boolean checkNumericCompatibility(@NotNull final Number value, @NotNull final Class<?> expected) {
        if (expected == Byte.class) {
            return Byte.MIN_VALUE <= value.longValue() && value.longValue() <= Byte.MAX_VALUE;
        } else if (expected == Short.class) {
            return Short.MIN_VALUE <= value.longValue() && value.longValue() <= Short.MAX_VALUE;
        } else if (expected == Integer.class) {
            return Integer.MIN_VALUE <= value.longValue() && value.longValue() <= Integer.MAX_VALUE;
        } else if (expected == Long.class) {
            return new BigInteger(value.toString()).compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0 &&
                    new BigInteger(value.toString()).compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0;
        } else if (expected == Float.class) {
            return value.getClass() != Double.class;
        } else if (expected == Double.class) {
            return value.getClass() != BigDecimal.class;
        } else {
            return expected == BigDecimal.class || expected == BigInteger.class;
        }
    }

    private static IterativeChunkedAggregationOperator makeAvgOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean exposeInternal) {
        if (type == Byte.class || type == byte.class) {
            return new ByteChunkedAvgOperator(name, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedAvgOperator(name, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedAvgOperator(name, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedAvgOperator(name, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedAvgOperator(name, exposeInternal);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedAvgOperator(name, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedAvgOperator(name, exposeInternal);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedAvgOperator(name, exposeInternal);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedAvgOperator(name, exposeInternal);
        } else if (AvgState.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException();
        } else if (AvgStateWithNan.class.isAssignableFrom(type)) {
            throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    private static IterativeChunkedAggregationOperator makeVarOrStdOperator(
            @NotNull final Class<?> type,
            @NotNull final String name,
            final boolean isStd,
            final boolean exposeInternal) {
        if (type == Byte.class || type == byte.class) {
            return new ByteChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Character.class || type == char.class) {
            return new CharChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Double.class || type == double.class) {
            return new DoubleChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Float.class || type == float.class) {
            return new FloatChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Integer.class || type == int.class) {
            return new IntChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Long.class || type == long.class) {
            return new LongChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == Short.class || type == short.class) {
            return new ShortChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == BigInteger.class) {
            return new BigIntegerChunkedVarOperator(isStd, name, exposeInternal);
        } else if (type == BigDecimal.class) {
            return new BigDecimalChunkedVarOperator(isStd, name, exposeInternal);
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
    }

    static IterativeChunkedAggregationOperator makeSortedFirstOrLastOperator(
            @NotNull final ChunkType chunkType,
            final boolean isFirst,
            final boolean multipleAggs,
            @NotNull final MatchPair[] resultPairs,
            @NotNull final QueryTable sourceTable) {
        if (sourceTable.isAddOnly()) {
            // @formatter:off
            switch (chunkType) {
                case Boolean: throw new UnsupportedOperationException("Columns never use boolean chunks");
                case    Char: return new CharAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case    Byte: return new ByteAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case   Short: return new ShortAddOnlySortedFirstOrLastChunkedOperator( isFirst, resultPairs, sourceTable, null);
                case     Int: return new IntAddOnlySortedFirstOrLastChunkedOperator(   isFirst, resultPairs, sourceTable, null);
                case    Long: return new LongAddOnlySortedFirstOrLastChunkedOperator(  isFirst, resultPairs, sourceTable, null);
                case   Float: return new FloatAddOnlySortedFirstOrLastChunkedOperator( isFirst, resultPairs, sourceTable, null);
                case  Double: return new DoubleAddOnlySortedFirstOrLastChunkedOperator(isFirst, resultPairs, sourceTable, null);
                case  Object: return new ObjectAddOnlySortedFirstOrLastChunkedOperator(isFirst, resultPairs, sourceTable, null);
            }
            // @formatter:on
        }
        if (sourceTable.isStream()) {
            // @formatter:off
            switch (chunkType) {
                case Boolean: throw new UnsupportedOperationException("Columns never use boolean chunks");
                case    Char: return new CharStreamSortedFirstOrLastChunkedOperator(  isFirst, multipleAggs, resultPairs, sourceTable);
                case    Byte: return new ByteStreamSortedFirstOrLastChunkedOperator(  isFirst, multipleAggs, resultPairs, sourceTable);
                case   Short: return new ShortStreamSortedFirstOrLastChunkedOperator( isFirst, multipleAggs, resultPairs, sourceTable);
                case     Int: return new IntStreamSortedFirstOrLastChunkedOperator(   isFirst, multipleAggs, resultPairs, sourceTable);
                case    Long: return new LongStreamSortedFirstOrLastChunkedOperator(  isFirst, multipleAggs, resultPairs, sourceTable);
                case   Float: return new FloatStreamSortedFirstOrLastChunkedOperator( isFirst, multipleAggs, resultPairs, sourceTable);
                case  Double: return new DoubleStreamSortedFirstOrLastChunkedOperator(isFirst, multipleAggs, resultPairs, sourceTable);
                case  Object: return new ObjectStreamSortedFirstOrLastChunkedOperator(isFirst, multipleAggs, resultPairs, sourceTable);
            }
            // @formatter:on
        }
        return new SortedFirstOrLastChunkedOperator(chunkType, isFirst, resultPairs, sourceTable);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Rollup Structure Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private static String makeRedirectionName(final int columnIdentifier) {
        return ROW_REDIRECTION_PREFIX + columnIdentifier + ROLLUP_COLUMN_SUFFIX;
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
            table.setAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, partitionOperator.getTableMap());
            if (reaggregated || includeConstituents) {
                table.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
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
        table.setAttribute(HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, EmptyTableMap.INSTANCE);
        table.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    }
}
