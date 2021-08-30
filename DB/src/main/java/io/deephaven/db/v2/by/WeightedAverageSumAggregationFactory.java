package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class WeightedAverageSumAggregationFactory implements AggregationContextFactory {
    private final String weightName;
    private final boolean isSum;

    public WeightedAverageSumAggregationFactory(final String weightName, boolean isSum) {
        this.weightName = weightName;
        this.isSum = isSum;
    }

    @Override
    public boolean allowKeyOnlySubstitution() {
        return true;
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
        final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>(operatorColumnCount + 1);
        final List<String> inputNames = new ArrayList<>(operatorColumnCount);
        final List<Boolean> isIntegerResult = new ArrayList<>(operatorColumnCount);
        final List<String> floatColumnNames = new ArrayList<>(operatorColumnCount);
        final List<String> integerColumnNames = new ArrayList<>(operatorColumnCount);

        final ColumnSource weightSource = withView.getColumnSource(weightName);
        boolean weightSourceIsFloatingPoint =
                weightSource.getChunkType() == ChunkType.Double || weightSource.getChunkType() == ChunkType.Float;
        boolean anyIntegerResults = !weightSourceIsFloatingPoint && isSum
                && withView.getColumnSourceMap().values().stream()
                        .anyMatch(cs -> cs.getChunkType() == ChunkType.Long || cs.getChunkType() == ChunkType.Int
                                || cs.getChunkType() == ChunkType.Short || cs.getChunkType() == ChunkType.Byte
                                || cs.getChunkType() == ChunkType.Char);
        boolean anyFloatResults = weightSourceIsFloatingPoint || !isSum || withView.getColumnSourceMap().values()
                .stream().anyMatch(cs -> cs.getChunkType() == ChunkType.Float || cs.getChunkType() == ChunkType.Double);

        final DoubleWeightRecordingInternalOperator doubleWeightOperator;
        if (anyFloatResults) {
            doubleWeightOperator = new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
            // noinspection unchecked
            inputColumns.add(weightSource);
            operators.add(doubleWeightOperator);
        } else {
            doubleWeightOperator = null;
        }

        final LongWeightRecordingInternalOperator longWeightOperator;
        if (anyIntegerResults) {
            longWeightOperator = new LongWeightRecordingInternalOperator(weightSource.getChunkType());
            // noinspection unchecked
            inputColumns.add(weightSource);
            operators.add(longWeightOperator);
        } else {
            longWeightOperator = null;
        }

        withView.getColumnSourceMap().forEach((name, columnSource) -> {
            if (groupByNames.contains(name)) {
                return;
            }
            if (name.equals(weightName)) {
                return;
            }

            // noinspection unchecked
            inputColumns.add(columnSource);
            inputNames.add(name);
            if (isSum) {
                final boolean isInteger;

                if (weightSourceIsFloatingPoint) {
                    isInteger = false;
                } else {
                    switch (columnSource.getChunkType()) {
                        case Char:
                        case Byte:
                        case Short:
                        case Int:
                        case Long:
                            isInteger = true;
                            break;
                        case Double:
                        case Float:
                            isInteger = false;
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Invalid chunk type for weightedSum: " + columnSource.getChunkType());
                    }
                }

                isIntegerResult.add(isInteger);
                if (isInteger) {
                    integerColumnNames.add(name);
                    operators.add(
                            new LongChunkedWeightedSumOperator(columnSource.getChunkType(), longWeightOperator, name));
                } else {
                    floatColumnNames.add(name);
                    operators.add(new DoubleChunkedWeightedSumOperator(columnSource.getChunkType(),
                            doubleWeightOperator, name));
                }
            } else {
                isIntegerResult.add(false);
                floatColumnNames.add(name);
                operators.add(
                        new ChunkedWeightedAverageOperator(columnSource.getChunkType(), doubleWeightOperator, name));
            }
        });

        final int inputColumnCount = inputNames.size();
        final int weightOperators = (anyFloatResults ? 1 : 0) + (anyIntegerResults ? 1 : 0);
        final String[][] inputNameArray = new String[inputColumnCount + weightOperators][];
        int idx = 0;
        if (doubleWeightOperator != null) {
            inputNameArray[idx] = new String[floatColumnNames.size() + 1];
            inputNameArray[idx][0] = weightName;
            for (int ii = 0; ii < floatColumnNames.size(); ++ii) {
                inputNameArray[idx][1 + ii] = floatColumnNames.get(ii);
            }
            idx++;
        }
        if (longWeightOperator != null) {
            inputNameArray[idx] = new String[integerColumnNames.size() + 1];
            inputNameArray[idx][0] = weightName;
            for (int ii = 0; ii < integerColumnNames.size(); ++ii) {
                inputNameArray[idx][1 + ii] = integerColumnNames.get(ii);
            }
            idx++;
        }

        for (final String columnName : inputNames) {
            inputNameArray[idx] = new String[2];
            inputNameArray[idx][0] = columnName;
            inputNameArray[idx][1] = weightName;
            idx++;
        }

        // noinspection unchecked
        return new AggregationContext(
                operators.toArray(
                        IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY),
                inputNameArray,
                inputColumns.toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY));
    }

    @Override
    public String toString() {
        return "Weighted" + (isSum ? "Sum" : "Avg") + "(" + weightName + ")";
    }

}
