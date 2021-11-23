package io.deephaven.engine.table.impl.by;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

public class CountAggregationFactory implements AggregationContextFactory {
    private final String resultName;
    private static final ChunkSource.WithPrev[] nullSourceArray = {null};

    public CountAggregationFactory(final String resultName) {
        this.resultName = NameValidator.validateColumnName(resultName);
    }

    @Override
    public AggregationContext makeAggregationContext(@NotNull final Table table,
            @NotNull final String... groupByColumns) {
        final IterativeChunkedAggregationOperator[] countOperator = new IterativeChunkedAggregationOperator[1];
        countOperator[0] = new CountAggregationOperator(resultName);

        final String[][] inputNameArray = new String[1][0];
        inputNameArray[0] = CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

        // noinspection unchecked
        return new AggregationContext(countOperator,
                inputNameArray,
                nullSourceArray);
    }

    @Override
    public String toString() {
        return "Count";
    }
}
