/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Aggregations;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.ColumnAggregations;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.FirstRowKey;
import io.deephaven.api.agg.LastRowKey;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.agg.Partition;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecAbsSum;
import io.deephaven.api.agg.spec.AggSpecApproximatePercentile;
import io.deephaven.api.agg.spec.AggSpecAvg;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import io.deephaven.api.agg.spec.AggSpecDistinct;
import io.deephaven.api.agg.spec.AggSpecFirst;
import io.deephaven.api.agg.spec.AggSpecFormula;
import io.deephaven.api.agg.spec.AggSpecFreeze;
import io.deephaven.api.agg.spec.AggSpecGroup;
import io.deephaven.api.agg.spec.AggSpecLast;
import io.deephaven.api.agg.spec.AggSpecMax;
import io.deephaven.api.agg.spec.AggSpecMedian;
import io.deephaven.api.agg.spec.AggSpecMin;
import io.deephaven.api.agg.spec.AggSpecPercentile;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecStd;
import io.deephaven.api.agg.spec.AggSpecSum;
import io.deephaven.api.agg.spec.AggSpecTDigest;
import io.deephaven.api.agg.spec.AggSpecUnique;
import io.deephaven.api.agg.spec.AggSpecVar;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.table.impl.by.rollup.NullColumns;
import io.deephaven.engine.table.impl.by.rollup.RollupAggregation;
import io.deephaven.engine.table.impl.by.rollup.RollupAggregationPairs;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ByteChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ByteRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.CharChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.CharRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.DoubleChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.DoubleRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.FloatChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.FloatRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.IntChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.IntRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.LongChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.LongRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ObjectChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ObjectRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ShortChunkedCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.count.ShortRollupCountDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ByteChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ByteRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.CharChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.CharRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.DoubleChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.DoubleRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.FloatChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.FloatRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.IntChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.IntRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.LongChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.LongRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ObjectChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ObjectRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ShortChunkedDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ShortRollupDistinctOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ByteChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ByteRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.CharChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.CharRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.DoubleChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.DoubleRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.FloatChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.FloatRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.IntChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.IntRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.LongChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.LongRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ObjectChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ObjectRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ShortChunkedUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.ShortRollupUniqueOperator;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.by.ssmpercentile.SsmChunkedPercentileOperator;
import io.deephaven.engine.table.impl.hierarchical.RollupTableImpl;
import io.deephaven.engine.table.impl.hierarchical.TreeTableImpl;
import io.deephaven.engine.table.impl.select.NullSelectColumn;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableConstantIntSource;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.util.freezeby.FreezeByCountOperator;
import io.deephaven.engine.table.impl.util.freezeby.FreezeByOperator;
import io.deephaven.time.DateTime;
import io.deephaven.util.FunctionalInterfaces.TriFunction;
import io.deephaven.util.annotations.FinalDefault;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_DOUBLE_ARRAY;
import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY;
import static io.deephaven.engine.table.ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY;
import static io.deephaven.engine.table.Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE;
import static io.deephaven.engine.table.impl.RollupAttributeCopier.DEFAULT_INSTANCE;
import static io.deephaven.engine.table.impl.RollupAttributeCopier.LEAF_WITHCONSTITUENTS_INSTANCE;
import static io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_DISTINCT_SSM_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_NAN_COUNT_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_NI_COUNT_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_NONNULL_COUNT_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_PI_COUNT_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_RUNNING_SUM2_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_RUNNING_SUM_COLUMN_ID;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROW_REDIRECTION_PREFIX;
import static io.deephaven.engine.table.impl.hierarchical.RollupTableImpl.KEY_WIDTH_COLUMN;
import static io.deephaven.engine.table.impl.hierarchical.RollupTableImpl.ROLLUP_COLUMN;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.type.TypeUtils.getBoxedType;
import static io.deephaven.util.type.TypeUtils.isNumeric;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Conversion tool to generate an {@link AggregationContextFactory} for a collection of {@link Aggregation
 * aggregations}.
 */
public class AggregationProcessor implements AggregationContextFactory {

    private enum Type {
        // @formatter:off
        NORMAL(false),
        ROLLUP_BASE(true),
        ROLLUP_REAGGREGATED(true),
        TREE_REVERSE_LOOKUP(false),
        SELECT_DISTINCT(false);
        // @formatter:on

        private final boolean isRollup;

        Type(boolean isRollup) {
            this.isRollup = isRollup;
        }
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
        if (aggregations.stream().anyMatch(agg -> agg instanceof Partition)) {
            rollupUnsupported("Partition");
        }
        final Collection<Aggregation> baseAggregations = new ArrayList<>(aggregations.size() + 1);
        baseAggregations.addAll(aggregations);
        baseAggregations.add(includeConstituents
                ? Partition.of(ROLLUP_COLUMN)
                : NullColumns.of(ROLLUP_COLUMN.name(), Object.class));
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
        if (aggregations.stream().anyMatch(agg -> agg instanceof Partition)) {
            rollupUnsupported("Partition");
        }
        final Collection<Aggregation> reaggregations = new ArrayList<>(aggregations.size() + 2);
        reaggregations.add(NullColumns.from(nullColumns));
        reaggregations.addAll(aggregations);
        reaggregations.add(Partition.of(ROLLUP_COLUMN));
        return new AggregationProcessor(reaggregations, Type.ROLLUP_REAGGREGATED);
    }

    /**
     * Create a trivial {@link AggregationContextFactory} to implement reverse lookup functionality for
     * {@link Table#tree(String, String) tree}.
     *
     * @return The {@link AggregationContextFactory}
     */
    public static AggregationContextFactory forTreeReverseLookup() {
        return new AggregationProcessor(Collections.emptyList(), Type.TREE_REVERSE_LOOKUP);
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
        final String duplicationErrorMessage = (type.isRollup
                ? RollupAggregationPairs.outputsOf(aggregations)
                : AggregationPairs.outputsOf(aggregations))
                        .collect(Collectors.groupingBy(ColumnName::name, Collectors.counting())).entrySet()
                        .stream()
                        .filter(kv -> kv.getValue() > 1)
                        .map(kv -> kv.getKey() + " used " + kv.getValue() + " times")
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
    public AggregationContext makeAggregationContext(
            @NotNull final Table table,
            final boolean requireStateChangeRecorder,
            @NotNull final String... groupByColumnNames) {
        switch (type) {
            case NORMAL:
                return new NormalConverter(table, requireStateChangeRecorder, groupByColumnNames).build();
            case ROLLUP_BASE:
                return new RollupBaseConverter(table, requireStateChangeRecorder, groupByColumnNames).build();
            case ROLLUP_REAGGREGATED:
                return new RollupReaggregatedConverter(table, requireStateChangeRecorder, groupByColumnNames).build();
            case TREE_REVERSE_LOOKUP:
                return makeTreeReverseLookupAggregationContext();
            case SELECT_DISTINCT:
                return makeEmptyAggregationContext(requireStateChangeRecorder);
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
        private final boolean requireStateChangeRecorder;
        final String[] groupByColumnNames;

        final boolean isAddOnly;
        final boolean isStream;

        final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
        final List<String[]> inputColumnNames = new ArrayList<>();
        final List<ChunkSource.WithPrev<Values>> inputSources = new ArrayList<>();
        final List<AggregationContextTransformer> transformers = new ArrayList<>();

        List<Pair> resultPairs = List.of();
        int freezeByCountIndex = -1;
        int trackedFirstOrLastIndex = -1;
        boolean partitionFound;

        private Converter(
                @NotNull final Table table,
                final boolean requireStateChangeRecorder,
                @NotNull final String... groupByColumnNames) {
            this.table = (QueryTable) table.coalesce();
            this.requireStateChangeRecorder = requireStateChangeRecorder;
            this.groupByColumnNames = groupByColumnNames;
            isAddOnly = this.table.isAddOnly();
            isStream = this.table.isStream();
        }

        AggregationContext build() {
            walkAllAggregations();
            return makeAggregationContext();
        }

        final void walkAllAggregations() {
            for (final Aggregation aggregation : aggregations) {
                aggregation.walk(this);
            }
        }

        @NotNull
        final AggregationContext makeAggregationContext() {
            if (requireStateChangeRecorder && operators.stream().noneMatch(op -> op instanceof StateChangeRecorder)) {
                addNoInputOperator(new CountAggregationOperator(null));
            }
            // noinspection unchecked
            return new AggregationContext(
                    operators.toArray(IterativeChunkedAggregationOperator[]::new),
                    inputColumnNames.toArray(String[][]::new),
                    inputSources.toArray(ChunkSource.WithPrev[]::new),
                    transformers.toArray(AggregationContextTransformer[]::new));
        }

        final void streamUnsupported(@NotNull final String operationName) {
            if (!isStream) {
                return;
            }
            throw new UnsupportedOperationException(String.format(
                    "Stream tables do not support Agg%s; use StreamTableTools.streamToAppendOnlyTable to accumulate full history",
                    operationName));
        }

        final void multiplePartitionsUnsupported() {
            if (!partitionFound) {
                partitionFound = true;
                return;
            }
            throw new UnsupportedOperationException("Only one AggPartition is permitted per aggregation");
        }

        // -------------------------------------------------------------------------------------------------------------
        // Partial Aggregation.Visitor (for cases common to all types)
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public final void visit(@NotNull final Aggregations aggregations) {
            aggregations.aggregations().forEach(a -> a.walk(this));
        }

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

        final void addApproximatePercentileOperators(final double percentile, final double compression) {
            for (final Pair pair : resultPairs) {
                final String inputName = pair.input().name();
                final String resultName = pair.output().name();

                addApproximatePercentileOperator(percentile, compression, inputName, resultName);
            }
        }

        final void addApproximatePercentileOperator(final double percentile, final double compression,
                @NotNull final String inputName, @NotNull final String resultName) {
            final ColumnSource<?> inputSource = table.getColumnSource(inputName);
            final Class<?> type = inputSource.getType();

            final int size = inputSources.size();
            for (int ii = 0; ii < size; ii++) {
                final IterativeChunkedAggregationOperator operator;
                if (inputSources.get(ii) == inputSource &&
                        (operator = operators.get(ii)) instanceof TDigestPercentileOperator) {
                    final TDigestPercentileOperator tDigestOperator = (TDigestPercentileOperator) operator;
                    if (tDigestOperator.compression() == compression) {
                        addOperator(tDigestOperator.makeSecondaryOperator(percentile, resultName), null,
                                inputName);
                        return;
                    }
                }
            }
            addOperator(new TDigestPercentileOperator(type, compression, percentile, resultName), inputSource,
                    inputName);
        }

        final void addFreezeOperators() {
            final FreezeByCountOperator countOperator;
            if (freezeByCountIndex >= 0) {
                countOperator = (FreezeByCountOperator) operators.get(freezeByCountIndex);
            } else {
                freezeByCountIndex = operators.size();
                addNoInputOperator(countOperator = new FreezeByCountOperator());
            }
            addBasicOperators((t, n) -> new FreezeByOperator(t, n, countOperator));
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

            final int size = inputSources.size();
            for (int ii = 0; ii < size; ii++) {
                if (inputSources.get(ii) != inputSource) {
                    continue;
                }
                final IterativeChunkedAggregationOperator operator = operators.get(ii);
                if (operator instanceof SsmChunkedMinMaxOperator) {
                    final SsmChunkedMinMaxOperator minMaxOperator = (SsmChunkedMinMaxOperator) operator;
                    addOperator(minMaxOperator.makeSecondaryOperator(isMin, resultName), null, inputName);
                    return;
                }
            }
            addOperator(makeMinOrMaxOperator(type, resultName, isMin, isAddOnly || isStream), inputSource, inputName);
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
            final List<WeightedOpResult> results = resultPairs.stream().map(pair -> {
                final ColumnSource<?> inputSource = table.getColumnSource(pair.input().name());
                final WeightedOpResultType resultType;
                if (isInteger(inputSource.getChunkType())) {
                    if (!weightSourceIsFloatingPoint && isSum) {
                        anyIntegerResults.setTrue();
                        resultType = WeightedOpResultType.INTEGER;
                    } else {
                        anyFloatingPointResults.setTrue();
                        resultType = WeightedOpResultType.FLOATING_POINT;
                    }
                } else if (isFloatingPoint(inputSource.getChunkType())) {
                    anyFloatingPointResults.setTrue();
                    resultType = WeightedOpResultType.FLOATING_POINT;
                } else {
                    throw new UnsupportedOperationException(
                            String.format("Invalid type %s in column %s for AggW%s weighted by %s",
                                    inputSource.getType(), pair.input().name(), isSum ? "Sum" : "Avg", weightName));
                }
                return new WeightedOpResult(pair, resultType, inputSource);
            }).collect(Collectors.toList());

            final LongWeightRecordingInternalOperator longWeightOperator;
            if (anyIntegerResults.booleanValue()) {
                longWeightOperator = new LongWeightRecordingInternalOperator(weightSource.getChunkType());
                addOperator(longWeightOperator, weightSource, Stream.of(weightName),
                        results.stream().filter(r -> r.type == WeightedOpResultType.INTEGER)
                                .map(r -> r.pair.input().name()));
            } else {
                longWeightOperator = null;
            }

            final DoubleWeightRecordingInternalOperator doubleWeightOperator;
            if (anyFloatingPointResults.booleanValue()) {
                doubleWeightOperator = new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
                addOperator(doubleWeightOperator, weightSource, Stream.of(weightName),
                        results.stream().filter(r -> r.type == WeightedOpResultType.FLOATING_POINT)
                                .map(r -> r.pair.input().name()));
            } else {
                doubleWeightOperator = null;
            }

            results.forEach(r -> {
                final IterativeChunkedAggregationOperator resultOperator;
                if (isSum) {
                    if (r.type == WeightedOpResultType.INTEGER) {
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

        private NormalConverter(
                @NotNull final Table table,
                final boolean requireStateChangeRecorder,
                @NotNull final String... groupByColumnNames) {
            super(table, requireStateChangeRecorder, groupByColumnNames);
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

        @Override
        public void visit(@NotNull final Partition partition) {
            multiplePartitionsUnsupported();
            streamUnsupported("Partition");
            addNoInputOperator(new PartitionByChunkedOperator(
                    table,
                    partition.includeGroupByColumns() ? table : (QueryTable) table.dropColumns(groupByColumnNames),
                    partition.column().name(),
                    (pt, st) -> pt.copyAttributes(st, BaseTable.CopyAttributeOperation.PartitionBy),
                    groupByColumnNames));
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, true));
        }

        @Override
        public void visit(@NotNull final AggSpecApproximatePercentile approxPct) {
            addApproximatePercentileOperators(approxPct.percentile(), approxPct.compression());
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            addBasicOperators((t, n) -> makeAvgOperator(t, n, false));
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
        public void visit(@NotNull final AggSpecFirst first) {
            addFirstOrLastOperators(true, null);
        }

        @Override
        public void visit(@NotNull final AggSpecFormula formula) {
            streamUnsupported("Formula");
            final GroupByChunkedOperator groupByChunkedOperator = new GroupByChunkedOperator(table, false,
                    resultPairs.stream().map(pair -> MatchPair.of((Pair) pair.input())).toArray(MatchPair[]::new));
            final FormulaChunkedOperator formulaChunkedOperator = new FormulaChunkedOperator(groupByChunkedOperator,
                    true, formula.formula(), formula.paramToken(), MatchPair.fromPairs(resultPairs));
            addNoInputOperator(formulaChunkedOperator);
        }

        @Override
        public void visit(AggSpecFreeze freeze) {
            addFreezeOperators();
        }

        @Override
        public void visit(@NotNull final AggSpecGroup group) {
            streamUnsupported("Group");
            addNoInputOperator(new GroupByChunkedOperator(table, true, MatchPair.fromPairs(resultPairs)));
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
            addBasicOperators((t, n) -> new SsmChunkedPercentileOperator(t, 0.50d, median.averageEvenlyDivided(), n));
        }

        @Override
        public void visit(@NotNull final AggSpecMin min) {
            addMinOrMaxOperators(true);
        }

        @Override
        public void visit(@NotNull final AggSpecPercentile pct) {
            addBasicOperators(
                    (t, n) -> new SsmChunkedPercentileOperator(t, pct.percentile(), pct.averageEvenlyDivided(), n));
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

        public void visit(@NotNull final AggSpecTDigest tDigest) {
            addBasicOperators((t, n) -> new TDigestPercentileOperator(t, tDigest.compression(), n,
                    ZERO_LENGTH_DOUBLE_ARRAY, ZERO_LENGTH_STRING_ARRAY));
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
        default void visit(@NotNull final AggSpecApproximatePercentile approxPct) {
            rollupUnsupported("ApproximatePercentile");
        }

        @Override
        default void visit(AggSpecFreeze freeze) {
            rollupUnsupported("Freeze");
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
        default void visit(@NotNull final AggSpecTDigest tDigest) {
            rollupUnsupported("TDigest");
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

        private int nextColumnIdentifier = 0;

        private RollupBaseConverter(
                @NotNull final Table table,
                final boolean requireStateChangeRecorder,
                @NotNull final String... groupByColumnNames) {
            super(table, requireStateChangeRecorder, groupByColumnNames);
        }

        @Override
        AggregationContext build() {
            walkAllAggregations();
            transformers.add(new StaticColumnSourceTransformer(
                    RollupTableImpl.KEY_WIDTH_COLUMN.name(),
                    new ImmutableConstantIntSource(groupByColumnNames.length)));
            transformers.add(new RowLookupAttributeSetter());
            return makeAggregationContext();
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
            multiplePartitionsUnsupported();
            streamUnsupported("Partition for rollup with constituents included");
            if (!partition.includeGroupByColumns()) {
                throw new UnsupportedOperationException("Rollups never drop group-by columns when partitioning");
            }

            final QueryTable adjustedTable = (QueryTable) table.updateView(
                    new NullSelectColumn<>(Table.class, null, ROLLUP_COLUMN.name()),
                    new NullSelectColumn<>(int.class, null, KEY_WIDTH_COLUMN.name()));
            final PartitionByChunkedOperator partitionOperator = new PartitionByChunkedOperator(table,
                    adjustedTable, partition.column().name(), LEAF_WITHCONSTITUENTS_INSTANCE, groupByColumnNames);

            addNoInputOperator(partitionOperator);
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            addBasicOperators((t, n) -> makeSumOperator(t, n, true));
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            addBasicOperators((t, n) -> makeAvgOperator(t, n, true));
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

        private RollupReaggregatedConverter(
                @NotNull final Table table,
                final boolean requireStateChangeRecorder,
                @NotNull final String... groupByColumnNames) {
            super(table, requireStateChangeRecorder, groupByColumnNames);
        }

        @Override
        AggregationContext build() {
            walkAllAggregations();
            transformers.add(new StaticColumnSourceTransformer(
                    RollupTableImpl.KEY_WIDTH_COLUMN.name(),
                    new ImmutableConstantIntSource(groupByColumnNames.length)));
            transformers.add(new RowLookupAttributeSetter());
            return makeAggregationContext();
        }

        // -------------------------------------------------------------------------------------------------------------
        // RollupAggregation.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final Count count) {
            addBasicOperators((t, n) -> new LongChunkedSumOperator(false, n));
        }

        @Override
        public void visit(@NotNull final NullColumns nullColumns) {
            transformers.add(new NullColumnAggregationTransformer(nullColumns.resultColumns()));
        }

        @Override
        public void visit(@NotNull final Partition partition) {
            multiplePartitionsUnsupported();
            if (!partition.includeGroupByColumns()) {
                throw new UnsupportedOperationException("Rollups never drop group-by columns when partitioning");
            }

            final List<String> columnsToDrop = table.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                    .filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).collect(Collectors.toList());
            final QueryTable adjustedTable = columnsToDrop.isEmpty()
                    ? table
                    : (QueryTable) table.dropColumns(columnsToDrop);
            final PartitionByChunkedOperator partitionOperator = new PartitionByChunkedOperator(
                    table, adjustedTable, partition.column().name(), DEFAULT_INSTANCE, groupByColumnNames);

            addNoInputOperator(partitionOperator);
        }

        // -------------------------------------------------------------------------------------------------------------
        // AggSpec.Visitor
        // -------------------------------------------------------------------------------------------------------------

        @Override
        public void visit(@NotNull final AggSpecAbsSum absSum) {
            reaggregateAsSum();
        }

        @Override
        public void visit(@NotNull final AggSpecAvg avg) {
            reaggregateAvgOperator();
        }

        @Override
        public void visit(@NotNull final AggSpecCountDistinct countDistinct) {
            reaggregateSsmBackedOperator((ssmSrc, priorResultSrc, n) -> makeCountDistinctOperator(
                    ssmSrc.getComponentType(), n, countDistinct.countNulls(), true, true));
        }

        @Override
        public void visit(@NotNull final AggSpecDistinct distinct) {
            reaggregateSsmBackedOperator((ssmSrc, priorResultSrc, n) -> makeDistinctOperator(
                    priorResultSrc.getComponentType(), n, distinct.includeNulls(), true, true));
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
            reaggregateSsmBackedOperator((ssmSrc, priorResultSrc, n) -> makeUniqueOperator(
                    priorResultSrc.getType(), n, unique.includeNulls(), null, unique.nonUniqueSentinel(), true, true));
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
                TriFunction<ColumnSource<SegmentedSortedMultiSet<?>>, ColumnSource<?>, String, IterativeChunkedAggregationOperator> operatorFactory) {
            for (final Pair pair : resultPairs) {
                final String resultName = pair.output().name();
                final String ssmName = resultName + ROLLUP_DISTINCT_SSM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                final ColumnSource<SegmentedSortedMultiSet<?>> ssmSource = table.getColumnSource(ssmName);
                final ColumnSource<?> priorResultSource = table.getColumnSource(resultName);
                final IterativeChunkedAggregationOperator operator = operatorFactory.apply(
                        ssmSource, priorResultSource, resultName);

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

                final String nanCountName = resultName + ROLLUP_NAN_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;

                if (table.hasColumns(nanCountName)) {
                    final DoubleChunkedSumOperator runningSumOp = addAndGetDoubleSumOperator(runningSumName);

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

                final String nanCountName = resultName + ROLLUP_NAN_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;

                if (table.hasColumns(nanCountName)) {
                    final DoubleChunkedSumOperator runningSumOp = addAndGetDoubleSumOperator(runningSumName);
                    final DoubleChunkedSumOperator runningSum2Op = addAndGetDoubleSumOperator(runningSum2Name);

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

    private static AggregationContext makeTreeReverseLookupAggregationContext() {
        // NB: UniqueRowKeyChunkedOperator is a StateChangeRecorder
        Assert.assertion(StateChangeRecorder.class.isAssignableFrom(UniqueRowKeyChunkedOperator.class),
                "StateChangeRecorder.class.isAssignableFrom(UniqueRowKeyChunkedOperator.class)");
        // noinspection unchecked
        return new AggregationContext(
                new IterativeChunkedAggregationOperator[] {
                        new UniqueRowKeyChunkedOperator(TreeTableImpl.REVERSE_LOOKUP_ROW_KEY_COLUMN.name())},
                new String[][] {ZERO_LENGTH_STRING_ARRAY},
                new ChunkSource.WithPrev[] {null},
                new AggregationContextTransformer[] {new RowLookupAttributeSetter()});
    }

    private static AggregationContext makeEmptyAggregationContext(final boolean requireStateChangeRecorder) {
        if (requireStateChangeRecorder) {
            // noinspection unchecked
            return new AggregationContext(
                    new IterativeChunkedAggregationOperator[] {new CountAggregationOperator(null)},
                    new String[][] {ZERO_LENGTH_STRING_ARRAY},
                    new ChunkSource.WithPrev[] {null});
        }
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

    private enum WeightedOpResultType {
        INTEGER, FLOATING_POINT
    }

    private static class WeightedOpResult {

        private final Pair pair;
        private final WeightedOpResultType type;
        private final ColumnSource<?> source;

        private WeightedOpResult(@NotNull final Pair pair, @NotNull final WeightedOpResultType type,
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
        }
        final Object onsAsType = maybeConvertType(type, onlyNullsSentinel);
        final Object nusAsType = maybeConvertType(type, nonUniqueSentinel);
        return reaggregated
                ? new ObjectRollupUniqueOperator(type, resultName, includeNulls, onsAsType, nusAsType)
                : new ObjectChunkedUniqueOperator(type, resultName, includeNulls, exposeInternal, onsAsType, nusAsType);
    }

    private static void checkType(@NotNull final String name, @NotNull final String valueIntent,
            @NotNull Class<?> expected, final Object value) {
        expected = getBoxedType(expected);
        if (value != null && !expected.isAssignableFrom(value.getClass())) {
            if (isNumeric(expected) && isNumeric(value.getClass())) {
                if (isNumericallyCompatible((Number) value, expected)) {
                    return;
                }
                throw new IllegalArgumentException(
                        String.format("For result column %s the %s %s is out of range for %s",
                                name, valueIntent, value, expected.getName()));
            }
            throw new IllegalArgumentException(
                    String.format("For result column %s the %s must be of type %s but is %s",
                            name, valueIntent, expected.getName(), value.getClass().getName()));
        }
    }

    private static Object maybeConvertType(@NotNull Class<?> expected, final Object value) {
        // We expect that checkType was already called and didn't throw...
        if (value == null) {
            return null;
        }
        if (expected.isAssignableFrom(value.getClass())) {
            return value;
        }
        if (expected == BigInteger.class) {
            return NumericConverter.lookup(value.getClass()).toBigInteger((Number) value);
        }
        return NumericConverter.lookup(value.getClass()).toBigDecimal((Number) value);
    }

    private interface NumericConverter {
        BigInteger toBigInteger(@Nullable final Number value);

        BigDecimal toBigDecimal(@Nullable final Number value);

        private static NumericConverter lookup(@NotNull final Class<?> numberClass) {
            final IntegralType integralType = IntegralType.lookup(numberClass);
            if (integralType != null) {
                return integralType;
            }
            return FloatingPointType.lookup(numberClass);
        }
    }

    private enum IntegralType implements NumericConverter {
        // @formatter:off
        BYTE      (n -> BigInteger.valueOf(n.byteValue()),  MIN_BYTE,  MAX_BYTE ),
        SHORT     (n -> BigInteger.valueOf(n.shortValue()), MIN_SHORT, MAX_SHORT),
        INTEGER   (n -> BigInteger.valueOf(n.intValue()),   MIN_INT,   MAX_INT  ),
        LONG      (n -> BigInteger.valueOf(n.longValue()),  MIN_LONG,  MAX_LONG ),
        BIGINTEGER(n -> (BigInteger) n,                     null,      null     );
        // @formatter:on

        private final Function<Number, BigInteger> toBigInteger;
        private final BigInteger lowerBound;
        private final BigInteger upperBound;

        IntegralType(@NotNull final Function<Number, BigInteger> toBigInteger,
                @Nullable final Number lowerBound,
                @Nullable final Number upperBound) {
            this.toBigInteger = toBigInteger;
            this.lowerBound = toBigInteger(lowerBound);
            this.upperBound = toBigInteger(upperBound);
        }

        @Override
        public BigInteger toBigInteger(@Nullable final Number value) {
            return value == null ? null : toBigInteger.apply(value);
        }

        @Override
        public BigDecimal toBigDecimal(@Nullable final Number value) {
            return value == null ? null : new BigDecimal(toBigInteger.apply(value));
        }

        private boolean inRange(@Nullable final BigInteger value) {
            if (value == null) {
                return true;
            }
            return (lowerBound == null || lowerBound.compareTo(value) <= 0) &&
                    (upperBound == null || upperBound.compareTo(value) >= 0);
        }

        private static IntegralType lookup(@NotNull final Class<?> numberClass) {
            try {
                return valueOf(numberClass.getSimpleName().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private enum FloatingPointType implements NumericConverter {
        // @formatter:off
        FLOAT(     n -> BigDecimal.valueOf(n.floatValue()),   MIN_FINITE_FLOAT,  MAX_FINITE_FLOAT ),
        DOUBLE(    n -> BigDecimal.valueOf(n.doubleValue()), MIN_FINITE_DOUBLE, MAX_FINITE_DOUBLE),
        BIGDECIMAL(n -> (BigDecimal) n,                      null,              null             );
        // @formatter:on

        private final Function<Number, BigDecimal> toBigDecimal;
        private final BigDecimal lowerBound;
        private final BigDecimal upperBound;

        FloatingPointType(@NotNull final Function<Number, BigDecimal> toBigDecimal,
                @Nullable final Number lowerBound,
                @Nullable final Number upperBound) {
            this.toBigDecimal = toBigDecimal;
            this.lowerBound = toBigDecimal(lowerBound);
            this.upperBound = toBigDecimal(upperBound);
        }

        @Override
        public BigInteger toBigInteger(@Nullable final Number value) {
            return value == null ? null : toBigDecimal.apply(value).toBigIntegerExact();
        }

        @Override
        public BigDecimal toBigDecimal(@Nullable final Number value) {
            return value == null ? null : toBigDecimal.apply(value);
        }

        private boolean inRange(@Nullable final BigDecimal value) {
            if (value == null) {
                return true;
            }
            return (lowerBound == null || lowerBound.compareTo(value) <= 0) &&
                    (upperBound == null || upperBound.compareTo(value) >= 0);
        }

        private static FloatingPointType lookup(@NotNull final Class<?> numberClass) {
            try {
                return valueOf(numberClass.getSimpleName().toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private static boolean isNumericallyCompatible(@NotNull final Number value,
            @NotNull final Class<?> expected) {
        final NumericConverter valueConverter = NumericConverter.lookup(value.getClass());
        if (valueConverter == null) {
            // value is not a recognized type
            return false;
        }

        final IntegralType expectedIntegralType = IntegralType.lookup(expected);
        if (expectedIntegralType != null) {
            // expected is a recognized integral type, just check range as a big int
            try {
                return expectedIntegralType.inRange(valueConverter.toBigInteger(value));
            } catch (ArithmeticException e) {
                // value is a floating point number with a fractional part
                return false;
            }
        }

        final FloatingPointType expectedFloatingPointType = FloatingPointType.lookup(expected);
        if (expectedFloatingPointType == null) {
            // expected is not a recognized type
            return false;
        }

        // check range as a big decimal
        if (expectedFloatingPointType.inRange(valueConverter.toBigDecimal(value))) {
            return true;
        }

        // value might be out of range, or might not be finite...
        if (expectedFloatingPointType == FloatingPointType.BIGDECIMAL ||
                (valueConverter != FloatingPointType.FLOAT && valueConverter != FloatingPointType.DOUBLE)) {
            // no way to represent NaN or infinity, so value is just out of range
            return false;
        }

        // if we're not finite, we can cast to a float or double successfully
        return !Double.isFinite(value.doubleValue());
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
    // Rollup and Tree Structure Helpers
    // -----------------------------------------------------------------------------------------------------------------

    private static String makeRedirectionName(final int columnIdentifier) {
        return ROW_REDIRECTION_PREFIX + columnIdentifier + ROLLUP_COLUMN_SUFFIX;
    }

    private static class RowLookupAttributeSetter implements AggregationContextTransformer {

        private ToIntFunction<?> reverseLookup;

        private RowLookupAttributeSetter() {}

        @Override
        public QueryTable transformResult(@NotNull final QueryTable table) {
            table.setAttribute(AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, reverseLookup);
            return table;
        }

        @Override
        public void supplyRowLookup(@NotNull final Supplier<ToIntFunction<Object>> rowLookupFactory) {
            this.reverseLookup = rowLookupFactory.get();
        }
    }
}
