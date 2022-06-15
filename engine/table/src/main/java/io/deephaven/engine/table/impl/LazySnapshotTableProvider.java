/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

/**
 * The right hand side table of a V2 snapshot must be an QueryTable (otherwise it fails with a class cast exception),
 * but there are certain cases where we may not want that to be true (e.g., a WnUnsubscribedTable). If so, the right
 * side table should implement this interface, and return a LazySnapshotTable for use with the snapshot.
 */
public interface LazySnapshotTableProvider {
    LazySnapshotTable getLazySnapshotTable();
}
