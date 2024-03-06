//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
    public final <T> T walk(TableSpec.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
