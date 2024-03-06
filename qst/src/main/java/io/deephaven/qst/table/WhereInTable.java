//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#whereIn(Object, Collection)
 * @see TableOperations#whereNotIn(Object, Collection)
 */
@Immutable
@NodeStyle
public abstract class WhereInTable extends TableBase {

    public static Builder builder() {
        return ImmutableWhereInTable.builder();
    }

    public abstract TableSpec left();

    public abstract TableSpec right();

    public abstract List<JoinMatch> matches();

    public abstract boolean inverted();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkNotEmpty() {
        if (matches().isEmpty()) {
            throw new IllegalArgumentException("Must not be empty");
        }
    }

    public interface Builder {

        Builder left(TableSpec left);

        Builder right(TableSpec right);

        Builder addMatches(JoinMatch element);

        Builder addMatches(JoinMatch... elements);

        Builder addAllMatches(Iterable<? extends JoinMatch> elements);

        Builder inverted(boolean inverted);

        WhereInTable build();
    }
}
