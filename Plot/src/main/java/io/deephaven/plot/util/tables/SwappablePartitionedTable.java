//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.tables;

/**
 * {@link SwappableTable} backed by a {@link PartitionedTableHandle}
 */
public interface SwappablePartitionedTable {

    PartitionedTableHandle getPartitionedTableHandle();
}
