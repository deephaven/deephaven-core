//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.PartitionedTableLocationKey;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider.LOCATION_ID_ATTR;

public final class TableBackedTableLocationKey extends PartitionedTableLocationKey {

    private static final String NAME = TableBackedTableLocationKey.class.getSimpleName();

    final QueryTable table;

    public TableBackedTableLocationKey(
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final QueryTable table) {
        super(partitions);
        this.table = table;
    }

    public QueryTable table() {
        return table;
    }

    @Override
    public String getImplementationName() {
        return NAME;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(NAME).append('[').append(table).append(']');
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (this == other) {
            return 0;
        }
        if (other instanceof TableBackedTableLocationKey) {
            final TableBackedTableLocationKey otherTyped = (TableBackedTableLocationKey) other;
            final int partitionComparisonResult =
                    PartitionsComparator.INSTANCE.compare(partitions, otherTyped.partitions);
            if (partitionComparisonResult != 0) {
                return partitionComparisonResult;
            }
            if (table == otherTyped.table) {
                return 0;
            }
            final int idComparisonResult = Integer.compare(getId(), otherTyped.getId());
            if (idComparisonResult != 0) {
                return idComparisonResult;
            }
            throw new UnsupportedOperationException(getImplementationName() +
                    " cannot be compared to instances that have different tables but the same \"" + LOCATION_ID_ATTR +
                    "\" attribute");
        }
        return super.compareTo(other);
    }

    private int getId() {
        // noinspection DataFlowIssue
        return (int) table.getAttribute(LOCATION_ID_ATTR);
    }

    @Override
    public int hashCode() {
        if (cachedHashCode == 0) {
            final int computedHashCode = 31 * partitions.hashCode() + Integer.hashCode(getId());
            // Don't use 0; that's used by StandaloneTableLocationKey, and also our sentinel for the need to compute
            if (computedHashCode == 0) {
                final int fallbackHashCode = TableBackedTableLocationKey.class.hashCode();
                cachedHashCode = fallbackHashCode == 0 ? 1 : fallbackHashCode;
            } else {
                cachedHashCode = computedHashCode;
            }
        }
        return cachedHashCode;
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        return other == this ||
                (other instanceof TableBackedTableLocationKey
                        && ((TableBackedTableLocationKey) other).table == table
                        && partitions.equals(((TableBackedTableLocationKey) other).partitions));
    }
}
