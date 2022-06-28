package io.deephaven.api.updateBy.spec;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateBy.ColumnUpdateClause;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

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
     * Build a {@link ColumnUpdateClause} for this UpdateBySpec.
     *
     * @param pair The input/output column name pair
     * @return The clause
     */
    ColumnUpdateClause clause(String pair);

    /**
     * Build a {@link ColumnUpdateClause} for this UpdateBySpec.
     *
     * @param pair The input/output column name pair
     * @return The clause
     */
    ColumnUpdateClause clause(Pair pair);

    /**
     * Build a {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateClause clause(String... pairs);

    /**
     * Build a {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateClause clause(Pair... pairs);

    /**
     * Build a {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    ColumnUpdateClause clause(Collection<? extends Pair> pairs);

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
