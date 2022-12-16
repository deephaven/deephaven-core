/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.object.UnionObject;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * Specifies an aggregation that outputs the single unique input value for groups that have one, {@code null} if all
 * input values are {@code null}, or {@link #nonUniqueSentinel()} if there is more than one distinct value.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecUnique extends AggSpecBase {

    public static final boolean INCLUDE_NULLS_DEFAULT = false;

    /**
     * Specify a "unique" aggregation that does not treat {@code null} as a value for purposes of determining if the
     * values in a group are unique. If a group is non-empty but contains only {@code null} values, its result will be
     * {@code null}. If a group contains more than a single unique value, its result will also be {@code null}.
     * 
     * @return The "unique" aggregation specification
     */
    public static AggSpecUnique of() {
        return ImmutableAggSpecUnique.builder().build();
    }

    /**
     * Equivalent to {@code of(includeNulls, UnionObject.from(nonUniqueSentinel))}.
     *
     * @param includeNulls Whether {@code null} is treated as a value for determining if the values in a group are
     *        unique
     * @param nonUniqueSentinel Sentinel value to use if a group contains more than a single unique value
     * @return The "unique" aggregation specification
     * @see UnionObject#from(Object)
     */
    public static AggSpecUnique of(boolean includeNulls, Object nonUniqueSentinel) {
        return of(includeNulls, UnionObject.from(nonUniqueSentinel));
    }

    /**
     * Specify a "unique" aggregation that optionally treats {@code null} as a value for purposes of determining if the
     * values in a group are unique. If a group is non-empty but contains only {@code null} values, its result will be
     * {@code null}. If a group contains more than a single unique value, its result will be {@code nonUniqueSentinel}.
     *
     * @param includeNulls Whether {@code null} is treated as a value for determining if the values in a group are
     *        unique
     * @param nonUniqueSentinel Sentinel value to use if a group contains more than a single unique value
     * @return The "unique" aggregation specification
     */
    public static AggSpecUnique of(boolean includeNulls, UnionObject nonUniqueSentinel) {
        ImmutableAggSpecUnique.Builder builder = ImmutableAggSpecUnique.builder().includeNulls(includeNulls);
        if (nonUniqueSentinel != null) {
            builder.nonUniqueSentinel(nonUniqueSentinel);
        }
        return builder.build();
    }

    @Override
    public final String description() {
        return "unique" + (includeNulls() ? " (including nulls)" : " (excluding nulls)");
    }

    /**
     * Whether to include {@code null} values as a distinct value for determining if there is only one unique value to
     * output.
     *
     * @return Whether to include nulls
     */
    @Default
    public boolean includeNulls() {
        return INCLUDE_NULLS_DEFAULT;
    }

    /**
     * The output value to use for groups that don't have a single unique input value.
     *
     * @return The non-unique sentinel value
     */
    public abstract Optional<UnionObject> nonUniqueSentinel();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
