//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

/**
 * An object for controlling the behavior of a {@code match} operation. Used by {@code ColumnSource#match} and when
 * creating {@code MatchFilter}.
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
     * In the case of string matching, whether the match should ignore case.
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

    /**
     * Return a clone of this {@link MatchOptions} with {@link #inverted()} set to the supplied value.
     */
    public MatchOptions withInverted(boolean inverted) {
        return builder()
                .caseInsensitive(caseInsensitive())
                .nanMatch(nanMatch())
                .inverted(inverted).build();
    }

    /**
     * Get a new {@link Builder} for constructing {@link MatchOptions} objects.
     */
    public static Builder builder() {
        return ImmutableMatchOptions.builder();
    }

    /**
     * A class for constructing {@link MatchOptions} instances
     */
    public interface Builder {
        /**
         * Set {@link #inverted()} to the supplied value
         */
        Builder inverted(boolean inverted);

        /**
         * Set {@link #caseInsensitive()} to the supplied value
         */
        Builder caseInsensitive(boolean caseInsensitive);

        /**
         * Set {@link #nanMatch()} to the supplied value
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
