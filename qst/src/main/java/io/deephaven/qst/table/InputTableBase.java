package io.deephaven.qst.table;

import java.util.UUID;

public abstract class InputTableBase extends TableBase implements InputTable {

    /**
     * An internal identifier used to break equivalence. Useful so that multiple input tables with the same basic
     * attributes can be created multiple times.
     *
     * @return the id
     */
    abstract UUID id();

    @Override
    public final <V extends TableSpec.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
