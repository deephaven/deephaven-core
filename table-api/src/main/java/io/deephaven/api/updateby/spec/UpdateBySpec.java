package io.deephaven.api.updateby.spec;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.ColumnUpdateOperation;

import java.util.Collection;

/**
 * A Specification for an updateBy operation. Implementations of this are essentially tagging classes for the underlying
 * visitor classes to walk to produce a final operation.
 */
public interface UpdateBySpec {
    /**
     * Determine if this spec can be applied to the specified type
     *
     * @param inputType the specified input type
     * @return true if this spec can be applied to the specified input type
     */
    boolean applicableTo(final Class<?> inputType);

    /**
     * Build a {@link ColumnUpdateOperation} for this UpdateBySpec.
     *
     * @param pair The input/output column name pair
     * @return The clause
     */
    ColumnUpdateOperation clause(String pair);

    /**
     * Build a {@link ColumnUpdateOperation} for this UpdateBySpec.
     *
     * @param pair The input/output column name pair
     * @return The clause
     */
    ColumnUpdateOperation clause(Pair pair);

    /**
     * Build a {@link ColumnUpdateOperation} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateOperation clause(String... pairs);

    /**
     * Build a {@link ColumnUpdateOperation} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateOperation clause(Pair... pairs);

    /**
     * Build a {@link ColumnUpdateOperation} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateOperation clause(Collection<? extends Pair> pairs);

    // region Visitor
    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(EmaSpec ema);

        T visit(FillBySpec f);

        T visit(CumSumSpec c);

        T visit(CumMinMaxSpec m);

        T visit(CumProdSpec p);
    }
    // endregion
}
