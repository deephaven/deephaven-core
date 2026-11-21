//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;
import java.util.function.Function;

/**
 * The options for a keyed aggregation, beyond its aggregations and group-by columns, for
 * {@link io.deephaven.api.TableOperations#aggBy(java.util.Collection, java.util.Collection, AggregationOptions)}.
 *
 * @param <TABLE> the table type of the {@link #initialGroups() initial groups}
 */
@Immutable
@BuildableStyle
public abstract class AggregationOptions<TABLE> {

    /**
     * @return the options that preserve no empty groups, have no initial groups, and use the
     *         {@link StateReclamation#configured() configured} reclamation
     * @param <TABLE> the table type of the initial groups
     */
    public static <TABLE> AggregationOptions<TABLE> defaults() {
        return AggregationOptions.<TABLE>builder().build();
    }

    /**
     * @return a builder of options
     * @param <TABLE> the table type of the initial groups
     */
    public static <TABLE> Builder<TABLE> builder() {
        return ImmutableAggregationOptions.builder();
    }

    /**
     * Whether to keep result rows for groups that are initially empty or become empty as a result of updates. Each
     * aggregation operator defines its own value for empty groups. Defaults to {@code false}.
     *
     * @return whether empty groups are preserved
     */
    @Default
    public boolean preserveEmpty() {
        return false;
    }

    /**
     * A table whose distinct combinations of values for the group-by columns are used to create an initial set of
     * aggregation groups. All other columns are ignored. This is useful in combination with {@link #preserveEmpty()} to
     * ensure that particular groups appear in the result table, or without it to control the encounter order for a
     * collection of groups and thus their relative order in the result. Changes to the initial groups are not expected
     * or handled; if they are a refreshing table, only its contents at instantiation time are used. Without initial
     * groups, the result is the same as if a table with no rows was supplied.
     *
     * @return the initial groups
     */
    public abstract Optional<TABLE> initialGroups();

    /**
     * How the states of groups whose rows have all been removed are reclaimed. Defaults to
     * {@link StateReclamation#configured()}.
     *
     * @return the state reclamation
     */
    @Default
    public StateReclamation stateReclamation() {
        return StateReclamation.configured();
    }

    /**
     * @param mapper converts the initial groups to another table type
     * @return these options, with the initial groups converted by {@code mapper}
     * @param <T> the other table type
     */
    public final <T> AggregationOptions<T> mapInitialGroups(final Function<? super TABLE, ? extends T> mapper) {
        final Builder<T> builder = AggregationOptions.<T>builder()
                .preserveEmpty(preserveEmpty())
                .stateReclamation(stateReclamation());
        if (initialGroups().isPresent()) {
            builder.initialGroups(mapper.apply(initialGroups().get()));
        }
        return builder.build();
    }

    public interface Builder<TABLE> {
        Builder<TABLE> preserveEmpty(boolean preserveEmpty);

        Builder<TABLE> initialGroups(TABLE initialGroups);

        Builder<TABLE> stateReclamation(StateReclamation stateReclamation);

        AggregationOptions<TABLE> build();
    }
}
