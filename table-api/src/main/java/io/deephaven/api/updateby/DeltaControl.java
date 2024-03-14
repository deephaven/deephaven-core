//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

/**
 * An object for controlling the behavior of a {@link UpdateByOperation#Delta(String...)}} operation.
 */
@Value.Immutable
@BuildableStyle
public abstract class DeltaControl {
    public static final DeltaControl NULL_DOMINATES = builder().nullDominates().build();
    public static final DeltaControl VALUE_DOMINATES = builder().valueDominates().build();
    public static final DeltaControl ZERO_DOMINATES = builder().zeroDominates().build();
    public static final DeltaControl DEFAULT = NULL_DOMINATES;

    /**
     * Get a new {@link Builder} for construction {@link DeltaControl} objects.
     * 
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return ImmutableDeltaControl.builder();
    }

    /**
     * Get the behavior of the Delta operation when null values are encountered.
     * 
     * @return the {@link NullBehavior}
     */
    @Value.Default
    public NullBehavior nullBehavior() {
        return NullBehavior.NullDominates;
    }

    /**
     * A class for constructing {@link DeltaControl} instances
     */
    public interface Builder {
        /**
         * Set the {@link NullBehavior} for the result to {@link NullBehavior#NullDominates}
         * 
         * @return this builder.
         */
        default Builder nullDominates() {
            this.nullBehavior(NullBehavior.NullDominates);
            return this;
        }

        /**
         * Set the {@link NullBehavior} for the result to {@link NullBehavior#ValueDominates}
         * 
         * @return this builder.
         */
        default Builder valueDominates() {
            this.nullBehavior(NullBehavior.ValueDominates);
            return this;
        }

        /**
         * Set the {@link NullBehavior} for the result to {@link NullBehavior#ZeroDominates}
         *
         * @return this builder.
         */
        default Builder zeroDominates() {
            this.nullBehavior(NullBehavior.ZeroDominates);
            return this;
        }

        /**
         * Set the {@link NullBehavior} for the result.
         * 
         * @return this builder.
         */
        Builder nullBehavior(final NullBehavior nullBehavior);

        /**
         * Construct a new immutable {@link DeltaControl} from this builder's state.
         * 
         * @return a new, immutable {@link DeltaControl}
         */
        DeltaControl build();
    }
}
