package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.literal.Literal;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class FilterMatches extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterMatches.builder();
    }

    public abstract ColumnName column();

    public abstract List<Literal> values();

    @Default
    public boolean caseInsensitive() {
        return false;
    }

    @Override
    public final FilterNot<FilterMatches> invert() {
        return FilterNot.of(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder column(ColumnName column);

        Builder caseInsensitive(boolean caseInsensitive);

        Builder addValues(Literal elements);

        Builder addValues(Literal... elements);

        Builder addAllValues(Iterable<? extends Literal> elements);

        FilterMatches build();
    }
}
