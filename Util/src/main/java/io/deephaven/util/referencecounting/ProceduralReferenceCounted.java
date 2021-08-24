package io.deephaven.util.referencecounting;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link ReferenceCounted} implementation that takes a {@link Runnable} onReferenceCountAtZero
 * procedure, in order to avoid relying on inheritance where necessary or desirable.
 */
public final class ProceduralReferenceCounted extends ReferenceCounted {

    /**
     * Callback procedure to be invoked by {@link #onReferenceCountAtZero()}.
     */
    private final Runnable onReferenceCountAtZeroProcedure;

    public ProceduralReferenceCounted(@NotNull final Runnable onReferenceCountAtZeroProcedure,
        final int initialValue) {
        super(initialValue);
        this.onReferenceCountAtZeroProcedure =
            Require.neqNull(onReferenceCountAtZeroProcedure, "onReferenceCountAtZeroProcedure");
    }

    public ProceduralReferenceCounted(@NotNull final Runnable onReferenceCountAtZeroProcedure) {
        this(onReferenceCountAtZeroProcedure, 0);
    }

    @Override
    protected final void onReferenceCountAtZero() {
        onReferenceCountAtZeroProcedure.run();
    }
}
