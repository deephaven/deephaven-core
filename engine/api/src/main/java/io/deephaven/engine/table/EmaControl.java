package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

import java.math.MathContext;
import java.util.Collection;

/**
 * <p>
 * Control parameters for performing EMAs with {@link Table#updateBy(Collection, String...)}
 * </p>
 * <p>
 * Defaults are as follows
 * </p>
 * <ul>
 * <li>On null Values - Skip</li>
 * <li>On NaN Values - Skip</li>
 * <li>On null timestamps - Skip</li>
 * <li>On zero delta Time - Skip</li>
 * <li>On negative delta time - Throw Exception</li>
 * <li>BigDecimal / BigInteger MathContext - Decimal 128</li>
 * </ul>
 */
public class EmaControl {
    public final BadDataBehavior onNullValue;
    public final BadDataBehavior onNanValue;
    public final BadDataBehavior onNullTime;
    public final BadDataBehavior onZeroDeltaTime;
    public final BadDataBehavior onNegativeDeltaTime;
    public final MathContext bigValueContext;

    @NotNull
    public static Builder get() {
        return new Builder();
    }

    private EmaControl(@NotNull final BadDataBehavior onNullValue,
            @NotNull final BadDataBehavior onNanValue,
            @NotNull final BadDataBehavior onNullTime,
            BadDataBehavior onZeroDeltaTime, BadDataBehavior onNegativeDeltaTime,
            @NotNull final MathContext bigValueContext) {
        this.onNullValue = onNullValue;
        this.onNanValue = onNanValue;
        this.onNullTime = onNullTime;
        this.onZeroDeltaTime = onZeroDeltaTime;
        this.onNegativeDeltaTime = onNegativeDeltaTime;
        this.bigValueContext = bigValueContext;
    }

    /**
     * Get the behavior for when null values are encountered.
     * 
     * @return the behavior for null values.
     */
    public BadDataBehavior getOnNullValue() {
        return onNullValue;
    }

    /**
     * Get the behavior for when NaN values are encountered.
     * 
     * @return the behavior for NaN values
     */
    public BadDataBehavior getOnNanValue() {
        return onNanValue;
    }

    /**
     * Get the behavior for when null timestamps are encountered.
     * 
     * @return the behavior for null timestamps.
     */
    public BadDataBehavior getOnNullTime() {
        return onNullTime;
    }

    /**
     * Get the behavior for when negative sample-to-sample time differences are encountered
     * 
     * @return the behavior for when dt is negative
     */
    public BadDataBehavior getOnNegativeDeltaTime() {
        return onNegativeDeltaTime;
    }

    /**
     * Get the behavior for when zero sample-to-sample-time differences are encountered.
     * 
     * @return the behavior for when dt is zero
     */
    public BadDataBehavior getOnZeroDeltaTime() {
        return onZeroDeltaTime;
    }

    /**
     * Get the {@link MathContext} to use when processing {@link java.math.BigInteger} and {@link java.math.BigDecimal}
     * values.
     * 
     * @return the {@link MathContext}
     */
    public MathContext getBigValueContext() {
        return bigValueContext;
    }

    @Override
    public String toString() {
        return "EmaControl{" +
                "onNullValue=" + onNullValue +
                ", onNanValue=" + onNanValue +
                ", bigValueContext=" + bigValueContext +
                '}';
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private BadDataBehavior onNullValue = BadDataBehavior.Skip;
        private BadDataBehavior onNanValue = BadDataBehavior.Skip;
        private BadDataBehavior onNullTime = BadDataBehavior.Skip;
        private BadDataBehavior onZeroDeltaTime = BadDataBehavior.Skip;
        private BadDataBehavior onNegativeDeltaTime = BadDataBehavior.Throw;
        private MathContext bigValueContext = MathContext.DECIMAL128;

        /**
         * Set the behavior for when null samples are encountered. Defaults to {@link BadDataBehavior#Skip}.
         *
         * @param behavior the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder onNullValue(@NotNull final BadDataBehavior behavior) {
            this.onNullValue = behavior;
            return this;
        }

        /**
         * Set the behavior for when NaN samples are encountered. Defaults to {@link BadDataBehavior#Skip}.
         *
         * @param behavior the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder onNanValue(@NotNull final BadDataBehavior behavior) {
            this.onNanValue = behavior;
            return this;
        }

        /**
         * Set the behavior for when null timestamps are encountered. Defaults to {@link BadDataBehavior#Skip}.
         *
         * @param behavior the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder onNullTime(@NotNull final BadDataBehavior behavior) {
            this.onNullTime = behavior;
            return this;
        }

        /**
         * Set the behavior for when zero sample-to-sample times are encountered. Defaults to
         * {@link BadDataBehavior#Skip}.
         *
         * @param behavior the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder onZeroDeltaTime(@NotNull final BadDataBehavior behavior) {
            this.onZeroDeltaTime = behavior;
            return this;
        }

        /**
         * Set the behavior for when negative sample-to-sample times are encountered. Defaults to
         * {@link BadDataBehavior#Throw}.
         *
         * @param behavior the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder onNegativeDeltaTime(@NotNull final BadDataBehavior behavior) {
            this.onNegativeDeltaTime = behavior;
            return this;
        }

        /**
         * Set the {@link MathContext} to use for processing {@link java.math.BigDecimal} and
         * {@link java.math.BigInteger} types. Defaults to {@link MathContext#DECIMAL128}.
         *
         * @param bigValueContext the desired behavior
         * @return this builder
         */
        @NotNull
        public Builder bigValueContext(@NotNull final MathContext bigValueContext) {
            this.bigValueContext = bigValueContext;
            return this;
        }

        /**
         * Construct an {@link EmaControl} from this builder.
         * 
         * @return
         */
        @NotNull
        public EmaControl build() {
            return new EmaControl(onNullValue, onNanValue, onNullTime, onZeroDeltaTime, onNegativeDeltaTime,
                    bigValueContext);
        }
    }
}
