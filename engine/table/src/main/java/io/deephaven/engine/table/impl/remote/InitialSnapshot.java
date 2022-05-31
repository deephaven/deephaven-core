/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.remote;

import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.rowset.RowSet;

import java.io.Serializable;

/**
 * A Raw table snapshot. Users may use {@link InitialSnapshotTable#setupInitialSnapshotTable(Table, InitialSnapshot)} to
 * convert this into a {@link io.deephaven.engine.table.impl.QueryTable}
 */
public class InitialSnapshot implements Serializable, Cloneable {
    static final long serialVersionUID = 4380513367437361741L;

    public Object type;
    public RowSet rowSet;
    public Object[] dataColumns;
    public long deltaSequence;
    public long step;
    public RowSet rowsIncluded;
    public RowSet viewport;

    public InitialSnapshot clone() {
        try {
            return (InitialSnapshot) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException();
        }
    }

    public InitialSnapshot setType(Object type) {
        this.type = type;
        return this;
    }

    public InitialSnapshot setViewport(RowSet viewport) {
        this.viewport = viewport;
        return this;
    }

    @Override
    public String toString() {
        return "InitialSnapshot{" +
                "type=" + type +
                ", rows=" + rowsIncluded + (rowSet == null ? "" : "/" + rowSet) +
                ", columns=" + FormatBitSet.formatBitSetAsString(FormatBitSet.arrayToBitSet(dataColumns)) +
                ", deltaSequence=" + deltaSequence +
                ", step=" + step +
                '}';
    }
}
