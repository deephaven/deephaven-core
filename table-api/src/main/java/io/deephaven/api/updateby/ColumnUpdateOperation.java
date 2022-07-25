package io.deephaven.api.updateby;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.spec.UpdateBySpec;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class ColumnUpdateOperation implements UpdateByOperation {
    public static Builder builder() {
        return ImmutableColumnUpdateOperation.builder();
    }


    /**
     * Provide the specification for an updateBy operation.
     */
    public abstract UpdateBySpec spec();

    /**
     * Provide the list of {@link Pair}s for the result columns. If `columns()` is not provided, internally will create
     * a new list mapping each source column 1:1 to output columns (where applicable)
     */
    public abstract List<Pair> columns();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder spec(UpdateBySpec spec);

        Builder addColumns(Pair element);

        Builder addColumns(Pair... elements);

        Builder addAllColumns(Iterable<? extends Pair> elements);

        ColumnUpdateOperation build();
    }
}
