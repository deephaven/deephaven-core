package io.deephaven.engine.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;
import io.deephaven.annotations.BuildableStyle;
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
@Immutable
@BuildableStyle
public abstract class EmaControl {
    @NotNull
    public static Builder builder() {
        return ImmutableEmaControl.builder();
    }

    public final static EmaControl DEFAULT = ImmutableEmaControl.builder().build();

    /**
     * Get the behavior for when null values are encountered.
     * 
     * @return the behavior for null values.
     */
    @Default
    public BadDataBehavior onNullValue() {
        return BadDataBehavior.Skip;
    }

    /**
     * Get the behavior for when NaN values are encountered.
     * 
     * @return the behavior for NaN values
     */
    @Default
    public BadDataBehavior onNanValue() {
        return BadDataBehavior.Skip;
    }

    /**
     * Get the behavior for when null timestamps are encountered.
     * 
     * @return the behavior for null timestamps.
     */
    @Default
    public BadDataBehavior onNullTime() {
        return BadDataBehavior.Skip;
    }

    /**
     * Get the behavior for when negative sample-to-sample time differences are encountered
     * 
     * @return the behavior for when dt is negative
     */
    @Default
    public BadDataBehavior onNegativeDeltaTime() {
        return BadDataBehavior.Throw;
    }

    /**
     * Get the behavior for when zero sample-to-sample-time differences are encountered.
     * 
     * @return the behavior for when dt is zero
     */
    @Default
    public BadDataBehavior onZeroDeltaTime() {
        return BadDataBehavior.Skip;
    }

    /**
     * Get the {@link MathContext} to use when processing {@link java.math.BigInteger} and {@link java.math.BigDecimal}
     * values.
     * 
     * @return the {@link MathContext}
     */
    @Default
    public MathContext bigValueContext() {
        return MathContext.DECIMAL128;
    }

    public interface Builder {
        Builder onNullValue(BadDataBehavior badDataBehavior);

        Builder onNanValue(BadDataBehavior badDataBehavior);

        Builder onNullTime(BadDataBehavior badDataBehavior);

        Builder onNegativeDeltaTime(BadDataBehavior badDataBehavior);

        Builder onZeroDeltaTime(BadDataBehavior badDataBehavior);

        Builder bigValueContext(MathContext context);

        EmaControl build();
    }
}
