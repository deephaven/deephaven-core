/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;

/**
 * This interface represents a table that will not update itself on the run cycle, but instead run itself when the run
 * for snapshot is called.
 */
public interface LazySnapshotTable extends Table {
    void refreshForSnapshot();
}
