package io.deephaven.qst.table;

/**
 * The system time provider.
 */
public enum TimeProviderSystem implements TimeProvider {
    INSTANCE;

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
