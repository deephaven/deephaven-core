package io.deephaven.db.v2.by;

import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ColumnSource;

import java.util.Map;
import java.util.function.ToIntFunction;

/**
 * Transformer to adjust aggregation results for operation building.
 */
public interface AggregationContextTransformer {

    AggregationContextTransformer[] ZERO_LENGTH_AGGREGATION_CONTEXT_TRANSFORMER_ARRAY =
            new AggregationContextTransformer[0];

    /**
     * After we have created the key columns, and the default result columns, allow each transformer to add additional
     * columns to the result set that are not handled by the regular modified column set transformer, etc. logic.
     */
    default void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {}

    /**
     * Before we return the result, each transformer has a chance to replace it or change it as it sees fit.
     *
     * Practically this is used to change the attributes for rollups.
     */
    default QueryTable transformResult(QueryTable table) {
        return table;
    }

    /**
     * The helper calls the transformer with a suitable reverse lookup function for this table.
     *
     * @param reverseLookup a function that translates an object to an integer position in our output.
     */
    default void setReverseLookupFunction(ToIntFunction<Object> reverseLookup) {}
}
