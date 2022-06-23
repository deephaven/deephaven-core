package io.deephaven.api.updateBy.spec;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateBy.ColumnUpdateClause;

import java.util.Arrays;
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
     * Build an {@link ColumnUpdateClause} for this UpdateBySpec.
     *
     * @param pair The input/output column name pair
     * @return The clause
     */
    default ColumnUpdateClause clause(Pair pair) {
        return ColumnUpdateClause.builder()
                .spec(this)
                .addColumns(pair)
                .build();
    }

    /**
     * Build an {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    default ColumnUpdateClause clause(String... pairs) {
        Pair[] arr = Arrays.stream(pairs)
                .map(Pair::parse)
                .toArray(Pair[]::new);

        return clause(arr);
    }

    /**
     * Build an {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    default ColumnUpdateClause clause(Pair... pairs) {
        if (pairs.length == 1) {
            return clause(pairs[0]);
        }
        return ColumnUpdateClause.builder().spec(this).addAllColumns(Arrays.asList(pairs)).build();
    }

    /**
     * Build an {@link ColumnUpdateClause} clause for this UpdateBySpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    default ColumnUpdateClause clause(Collection<? extends Pair> pairs) {
        if (pairs.size() == 1) {
            return clause(pairs.iterator().next());
        }
        return ColumnUpdateClause.builder().spec(this).addAllColumns(pairs).build();
    }

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
