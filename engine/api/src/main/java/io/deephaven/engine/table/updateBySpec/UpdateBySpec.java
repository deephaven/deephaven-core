package io.deephaven.engine.table.updateBySpec;

import org.jetbrains.annotations.NotNull;

/**
 * A Specification for an updateBy operation. Implementations of this are essentially tagging classes for the underlying
 * visitor classes to walk to produce a final operation.
 */
public interface UpdateBySpec {
    /**
     * Get a description of the operation requested.
     * 
     * @return a description of the operation
     */
    @NotNull
    String describe();

    /**
     * Determine if this spec can be applied to the specified type
     *
     * @param inputType
     * @return true if this spec can be applied to the specified input type
     */
    boolean applicableTo(@NotNull final Class<?> inputType);

    // region Visitor
    <V extends Visitor> V walk(final @NotNull V v);

    interface Visitor {
        void visit(EmaSpec ema);

        void visit(FillBySpec f);

        void visit(CumSumSpec c);

        void visit(CumMinMaxSpec m);

        void visit(CumProdSpec p);
    }
    // endregion
}
