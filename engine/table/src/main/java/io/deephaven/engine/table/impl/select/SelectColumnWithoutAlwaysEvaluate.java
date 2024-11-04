package io.deephaven.engine.table.impl.select;

/**
 * This is a base class for SelectColumn implementations that do not support {@link SelectColumn#alwaysEvaluate()}.
 *
 * <p>These columns always return false for {@link #alwaysEvaluate()} and {@link #alwaysEvaluateCopy()} calls {@link #copy()}.</p>
 */
public abstract class SelectColumnWithoutAlwaysEvaluate implements SelectColumn {
    @Override
    public boolean alwaysEvaluate() {
        return false;
    }

    @Override
    public SelectColumn alwaysEvaluateCopy() {
        return copy();
    }
}
