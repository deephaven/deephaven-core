package io.deephaven.engine.table.updateBySpec;

import org.jetbrains.annotations.NotNull;

/**
 * A Specification for an updateBy operation. Implementations of this are essentially tagging classes for the underlying
 * visitor classes to walk to produce a final operation.
 */
public interface UpdateBySpec {
    /**
     * Determine if this spec can be applied to the specified type
     *
     * @param inputType
     * @return true if this spec can be applied to the specified input type
     */
    boolean applicableTo(@NotNull final Class<?> inputType);

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
