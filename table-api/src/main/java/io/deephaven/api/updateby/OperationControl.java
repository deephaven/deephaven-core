package io.deephaven.api.updateby;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import io.deephaven.annotations.BuildableStyle;

import java.math.MathContext;
import java.util.Optional;

/**
 * <p>
 * Control parameters for performing operations with Table#updateBy()
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
@Immutable
@BuildableStyle
public abstract class OperationControl {
    public static Builder builder() {
        return ImmutableOperationControl.builder();
    }

    public static OperationControl defaultInstance() {
        return builder().build();
    }

    public abstract Optional<BadDataBehavior> onNullValue();

    public abstract Optional<BadDataBehavior> onNanValue();

    public abstract Optional<BadDataBehavior> onNullTime();

    public abstract Optional<BadDataBehavior> onNegativeDeltaTime();

    public abstract Optional<BadDataBehavior> onZeroDeltaTime();

    public abstract Optional<MathContext> bigValueContext();

    /**
     * Get the behavior for when {@code null} values are encountered. Defaults to {@link BadDataBehavior#SKIP SKIP}.
     * 
     * @return the behavior for {@code null} values.
     */
    @Value.Derived
    public BadDataBehavior onNullValueOrDefault() {
        return onNullValue().orElse(BadDataBehavior.SKIP);
    }

    /**
     * Get the behavior for when {@link Double#NaN} values are encountered. Defaults to {@link BadDataBehavior#SKIP
     * SKIP}.
     * 
     * @return the behavior for {@link Double#NaN} values
     */
    @Value.Derived
    public BadDataBehavior onNanValueOrDefault() {
        return onNanValue().orElse(BadDataBehavior.SKIP);
    }

    /**
     * Get the behavior for when {@code null} timestamps are encountered. Defaults to {@link BadDataBehavior#SKIP SKIP}.
     * 
     * @return the behavior for {@code null} timestamps.
     */
    @Value.Derived
    public BadDataBehavior onNullTimeOrDefault() {
        return onNullTime().orElse(BadDataBehavior.SKIP);
    }

    /**
     * Get the behavior for when negative sample-to-sample time differences are encountered. Defaults to
     * {@link BadDataBehavior#THROW THROW}.
     * 
     * @return the behavior for when dt is negative
     */
    @Value.Derived
    public BadDataBehavior onNegativeDeltaTimeOrDefault() {
        return onNegativeDeltaTime().orElse(BadDataBehavior.THROW);
    }

    /**
     * Get the behavior for when zero sample-to-sample-time differences are encountered. Defaults to
     * {@link BadDataBehavior#SKIP SKIP}.
     * 
     * @return the behavior for when dt is zero
     */
    @Value.Derived
    public BadDataBehavior onZeroDeltaTimeOrDefault() {
        return onZeroDeltaTime().orElse(BadDataBehavior.SKIP);
    }

    /**
     * Get the {@link MathContext} to use when processing {@link java.math.BigInteger} and {@link java.math.BigDecimal}
     * values. Defaults to {@link MathContext#DECIMAL128}.
     * 
     * @return the {@link MathContext}
     */
    @Value.Derived
    public MathContext bigValueContextOrDefault() {
        return bigValueContext().orElse(MathContext.DECIMAL128);
    }

    /**
     * Create a new instance with all of the explicit-or-default values from {@code this}. This may be useful from the
     * context of a client who wants to use client-side configuration defaults instead of server-side configuration
     * defaults.
     *
     * @return the explicit new instance
     */
    public final OperationControl materialize() {
        return builder()
                .onNullValue(onNullValueOrDefault())
                .onNanValue(onNanValueOrDefault())
                .onNullTime(onNullTimeOrDefault())
                .onNegativeDeltaTime(onNegativeDeltaTimeOrDefault())
                .onZeroDeltaTime(onZeroDeltaTimeOrDefault())
                .bigValueContext(bigValueContextOrDefault())
                .build();
    }

    public interface Builder {
        Builder onNullValue(BadDataBehavior badDataBehavior);

        Builder onNanValue(BadDataBehavior badDataBehavior);

        Builder onNullTime(BadDataBehavior badDataBehavior);

        Builder onNegativeDeltaTime(BadDataBehavior badDataBehavior);

        Builder onZeroDeltaTime(BadDataBehavior badDataBehavior);

        Builder bigValueContext(MathContext context);

        OperationControl build();
    }
}
