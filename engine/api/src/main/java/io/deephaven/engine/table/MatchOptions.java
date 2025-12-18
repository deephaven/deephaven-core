//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

/**
 * An object for controlling the behavior of a {@code match} operation. This includes {@link ColumnSource#match} and
 * creating {@code MatchFilters}.
 */
@Immutable
@BuildableStyle
public abstract class MatchOptions {
    public static final MatchOptions REGULAR = builder().build();
    public static final MatchOptions INVERTED = builder().inverted(true).build();

    /**
     * Whether the match should be inverted.
     */
    @Default
    public boolean inverted() {
        return false;
    }

    /**
     * In the case of string matching, whether the match should be case-sensitive.
     */
    @Default
    public boolean caseInsensitive() {
        return false;
    }

    /**
     * In the case of floating point matching, whether two NaN values are equivalent.
     */
    @Default
    public boolean nanMatch() {
        return false;
    }

    public MatchOptions withInverted(boolean inverted) {
        return builder()
                .caseInsensitive(caseInsensitive())
                .nanMatch(nanMatch())
                .inverted(inverted).build();
    }

    /**
     * Get a new {@link Builder} for construction {@link MatchOptions} objects.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return ImmutableMatchOptions.builder();
    }

    /**
     * A class for constructing {@link MatchOptions} instances
     */
    public interface Builder {
        /**
         * Set the {@link #inverted()} for the result to the supplied value
         *
         * @return this builder.
         */
        Builder inverted(boolean inverted);

        /**
         * Set the {@link #caseInsensitive()} for the result to the supplied value
         *
         * @return this builder.
         */
        Builder caseInsensitive(boolean caseInsensitive);

        /**
         * Set the {@link #nanMatch()} for the result to the supplied value
         *
         * @return this builder.
         */
        Builder nanMatch(boolean nanMatch);

        /**
         * Construct a new immutable {@link MatchOptions} from this builder's state.
         *
         * @return a new, immutable {@link MatchOptions}
         */
        MatchOptions build();
    }
}
