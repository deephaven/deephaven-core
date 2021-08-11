package io.deephaven.qst.table;

import io.deephaven.api.JoinMatch;
import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#whereIn(Object, Collection)
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

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
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

        WhereInTable build();
    }
}
