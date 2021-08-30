/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.remote;

import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.utils.Index;

import java.io.Serializable;

/**
 * A Raw table snapshot. Users may use
 * {@link InitialSnapshotTable#setupInitialSnapshotTable(Table, InitialSnapshot)} to convert this
 * into a {@link io.deephaven.db.v2.QueryTable}
 */
public class InitialSnapshot implements Serializable, Cloneable {
    static final long serialVersionUID = 4380513367437361741L;

    public Object type;
    public Index index;
    public Object[] dataColumns;
    public long deltaSequence;
    public long step;
    public Index rowsIncluded;
    public Index viewport;

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

    public InitialSnapshot setViewport(Index viewport) {
        this.viewport = viewport;
        return this;
    }

    @Override
    public String toString() {
        return "InitialSnapshot{" +
            "type=" + type +
            ", rows=" + rowsIncluded + (index == null ? "" : "/" + index) +
            ", columns="
            + FormatBitSet.formatBitSetAsString(FormatBitSet.arrayToBitSet(dataColumns)) +
            ", deltaSequence=" + deltaSequence +
            ", step=" + step +
            '}';
    }
}
