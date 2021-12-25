package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeightedAverageSumAggregationFactory implements AggregationContextFactory {
    private final String weightName;
    private final boolean isSum;

    public WeightedAverageSumAggregationFactory(final String weightName, boolean isSum) {
        this.weightName = weightName;
        this.isSum = isSum;
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
            @NotNull final String... groupByColumns) {
        return getAllColumnOperators(table, groupByColumns);
    }

    private AggregationContext getAllColumnOperators(Table withView, String[] groupByNameArray) {
        final Set<String> groupByNames = new HashSet<>(Arrays.asList(groupByNameArray));
        final int operatorColumnCount = withView.getColumnSourceMap().size() - groupByNames.size() - 1;

        final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>(operatorColumnCount + 1);
        final List<String[]> inputNames = new ArrayList<>(operatorColumnCount);
        final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>(operatorColumnCount + 1);

        final MatchPair[] resultPairs = withView.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(cn -> !cn.equals(weightName) && !groupByNames.contains(cn))
                .map(cn -> new MatchPair(cn, cn))
                .toArray(MatchPair[]::new);
        getOperatorsAndInputs(withView, weightName, isSum, resultPairs, operators, inputNames, inputColumns);

        // noinspection unchecked
        return new AggregationContext(
                operators.toArray(IterativeChunkedAggregationOperator[]::new),
                inputNames.toArray(String[][]::new),
                inputColumns.toArray(ChunkSource.WithPrev[]::new));
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
        private final MatchPair pair;
        private final ResultType type;
        private final ColumnSource<?> source;

        private Result(MatchPair pair, ResultType type, ColumnSource<?> source) {
            this.pair = pair;
            this.type = type;
            this.source = source;
        }
    }

    static void getOperatorsAndInputs(Table withView,
            String weightName,
            boolean isSum,
            MatchPair[] resultPairs,
            List<IterativeChunkedAggregationOperator> resultOperators,
            List<String[]> resultInputNames,
            List<ChunkSource.WithPrev<Values>> resultInputColumns) {
        final ColumnSource weightSource = withView.getColumnSource(weightName);
        final boolean weightSourceIsFloatingPoint;
        if (isInteger(weightSource.getChunkType())) {
            weightSourceIsFloatingPoint = false;
        } else if (isFloatingPoint(weightSource.getChunkType())) {
            weightSourceIsFloatingPoint = true;
        } else {
            throw new UnsupportedOperationException(String.format("Invalid type %s in weight column %s for %s",
                    weightSource.getType(), weightName, toString(isSum, weightName)));
        }

        final MutableBoolean anyIntegerResults = new MutableBoolean();
        final MutableBoolean anyFloatingPointResults = new MutableBoolean();
        final List<Result> results = Arrays.stream(resultPairs).map(pair -> {
            final ColumnSource<?> inputSource = withView.getColumnSource(pair.rightColumn);
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
                throw new UnsupportedOperationException(String.format("Invalid type %s in column %s for %s",
                        inputSource.getType(), pair.rightColumn, toString(isSum, weightName)));
            }
            return new Result(pair, resultType, inputSource);
        }).collect(Collectors.toList());

        final LongWeightRecordingInternalOperator longWeightOperator;
        if (anyIntegerResults.booleanValue()) {
            longWeightOperator = new LongWeightRecordingInternalOperator(weightSource.getChunkType());
            resultOperators.add(longWeightOperator);
            resultInputNames.add(Stream.concat(
                    Stream.of(weightName),
                    results.stream()
                            .filter(r -> r.type == ResultType.INTEGER).map(r -> r.pair.rightColumn))
                    .toArray(String[]::new));
            // noinspection unchecked
            resultInputColumns.add(weightSource);
        } else {
            longWeightOperator = null;
        }

        final DoubleWeightRecordingInternalOperator doubleWeightOperator;
        if (anyFloatingPointResults.booleanValue()) {
            doubleWeightOperator = new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
            resultOperators.add(doubleWeightOperator);
            resultInputNames.add(Stream.concat(
                    Stream.of(weightName),
                    results.stream()
                            .filter(r -> r.type == ResultType.FLOATING_POINT).map(r -> r.pair.rightColumn))
                    .toArray(String[]::new));
            // noinspection unchecked
            resultInputColumns.add(weightSource);
        } else {
            doubleWeightOperator = null;
        }

        results.forEach(r -> {
            if (isSum) {
                if (r.type == ResultType.INTEGER) {
                    resultOperators.add(new LongChunkedWeightedSumOperator(
                            r.source.getChunkType(), longWeightOperator, r.pair.leftColumn));
                } else {
                    resultOperators.add(new DoubleChunkedWeightedSumOperator(
                            r.source.getChunkType(), doubleWeightOperator, r.pair.leftColumn));
                }
            } else {
                resultOperators.add(new ChunkedWeightedAverageOperator(
                        r.source.getChunkType(), doubleWeightOperator, r.pair.leftColumn));
            }
            resultInputNames.add(new String[] {r.pair.rightColumn, weightName});
            resultInputColumns.add(r.source);
        });
    }

    @Override
    public String toString() {
        return toString(isSum, weightName);
    }

    private static String toString(final boolean isSum, final String weightName) {
        return "Weighted" + (isSum ? "Sum" : "Avg") + "(" + weightName + ")";
    }
}
