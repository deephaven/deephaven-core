/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;

/**
 * This interface represents a table that will not update itself on the refresh cycle, but instead refresh itself when
 * the refresh for snapshot is called.
 */
public interface LazySnapshotTable extends Table {
    void refreshForSnapshot();
}
