//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@NodeStyle
public abstract class SliceTable extends TableBase implements SingleParentTable {

    public static SliceTable of(TableSpec parent, long firstPositionInclusive, long lastPositionExclusive) {
        return ImmutableSliceTable.of(parent, firstPositionInclusive, lastPositionExclusive);
    }

    @Parameter
    public abstract TableSpec parent();

    @Parameter
    public abstract long firstPositionInclusive();

    @Parameter
    public abstract long lastPositionExclusive();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkPositions() {
        if (firstPositionInclusive() >= 0 && lastPositionExclusive() >= 0
                && lastPositionExclusive() < firstPositionInclusive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot slice with a non-negative start position (%d) that is after a non-negative end position (%d).",
                            firstPositionInclusive(), lastPositionExclusive()));
        }
        if (firstPositionInclusive() < 0 && lastPositionExclusive() < 0
                && lastPositionExclusive() < firstPositionInclusive()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot slice with a negative start position (%d) that is after a negative end position (%d).",
                            firstPositionInclusive(), lastPositionExclusive()));
        }
    }
}
