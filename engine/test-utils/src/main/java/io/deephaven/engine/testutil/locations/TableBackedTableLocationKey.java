//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.UnknownPartitionKeyException;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Set;

public final class TableBackedTableLocationKey implements ImmutableTableLocationKey {

    private static final String NAME = TableBackedTableLocationKey.class.getSimpleName();

    final QueryTable table;

    public TableBackedTableLocationKey(@NotNull final QueryTable table) {
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
        if (other instanceof TableBackedTableLocationKey) {
            final TableBackedTableLocationKey otherTyped = (TableBackedTableLocationKey) other;
            // noinspection DataFlowIssue
            final int idComparisonResult =
                    Integer.compare((int) table.getAttribute("ID"), (int) otherTyped.table.getAttribute("ID"));
            if (idComparisonResult != 0) {
                return idComparisonResult;
            }
        }
        return ImmutableTableLocationKey.super.compareTo(other);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(table);
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        return other == this ||
                (other instanceof TableBackedTableLocationKey
                        && ((TableBackedTableLocationKey) other).table == table);
    }

    @Override
    public <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(
            @NotNull final String partitionKey) {
        throw new UnknownPartitionKeyException(partitionKey, this);
    }

    @Override
    public Set<String> getPartitionKeys() {
        return Collections.emptySet();
    }
}
