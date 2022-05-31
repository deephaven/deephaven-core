package io.deephaven.engine.table.impl.util;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.SelectColumn;

import java.util.Collection;

/**
 * FreezeBy records the first value for a given key in the output table and ignores subsequent changes.
 *
 * When keys are removed, the corresponding row is removed from the output table. When keys are added back on another
 * cycle, the newly added value is frozen in the output.
 *
 * Only one row per key is allowed in the input. This is because the operation can not determine which is the correct
 * row to freeze in cases where there are multiple rows per key. The freeze operation is not sensitive to RowSet changes
 * (e.g., adds, removes, modifies, shifts); whether a row is updated is based solely on (1) a key did not exist in the
 * input table at the start of the cycle and (2) it now exists in the input table. If the key did not exist, a frozen
 * copy is taken. If the key did exist, then no modifications occur.
 */
public class FreezeBy {
    private FreezeBy() {} // static use only

    /**
     * Freeze the input table.
     *
     * <p>
     * The input table may only have zero or one rows. The first added row will be frozen until the table becomes empty.
     * </p>
     *
     * @param input the table to freeze
     * @return a frozen copy of the input table
     */
    public static Table freezeBy(Table input) {
        return input.aggAllBy(AggSpec.freeze());
    }

    /**
     * Freeze the input table.
     *
     * <p>
     * When a key is added to the table, a copy is added to the output. When a key is removed, the row is removed from
     * the output. The input may have only one row per key.
     * <p>
     *
     * @param input the table to freeze
     * @param groupByColumns the keys to freeze the table by
     *
     * @return a copy of the input table frozen by key
     */
    public static Table freezeBy(Table input, String... groupByColumns) {
        return input.aggAllBy(AggSpec.freeze(), groupByColumns);
    }

    /**
     * Freeze the input table.
     *
     * <p>
     * When a key is added to the table, a copy is added to the output. When a key is removed, the row is removed from
     * the output. The input may have only one row per key.
     * </p>
     *
     * @param input the table to freeze
     * @param groupByColumns the keys to freeze the table by
     *
     * @return a copy of the input table frozen by key
     */
    public static Table freezeBy(Table input, Collection<String> groupByColumns) {
        return input.aggAllBy(AggSpec.freeze(), groupByColumns);
    }

    /**
     * Freeze the input table.
     *
     * <p>
     * When a key is added to the table, a copy is added to the output. When a key is removed, the row is removed from
     * the output. The input may have only one row per key.
     * </p>
     *
     * @param input the table to freeze
     * @param groupByColumns the keys to freeze the table by
     *
     * @return a copy of the input table frozen by key
     */
    public static Table freezeBy(Table input, SelectColumn... groupByColumns) {
        return input.aggAllBy(AggSpec.freeze(), groupByColumns);
    }
}
