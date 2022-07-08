package io.deephaven.api.updateby;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import javax.annotation.Nullable;
import java.math.MathContext;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * An interface to control the behavior of an {@code Table#updateBy}
 */
@Immutable
@BuildableStyle
public abstract class UpdateByControl {
    public static Builder builder() {
        return ImmutableUpdateByControl.builder();
    }

    /**
     * Get an instance of UpdateByControl with the defaults from the system properties applied.
     *
     * @return default UpdateByControl
     */
    public static UpdateByControl defaultInstance() {
        return builder().build();
    }

    public static boolean useRedirectionDefault() {
        return Boolean.getBoolean("UpdateByControl.useRedirection");
    }

    public static int chunkCapacityDefault() {
        return Integer.getInteger("UpdateByControl.chunkCapacity", 4096);
    }

    public static double maximumStaticMemoryOverheadDefault() {
        return Double.parseDouble(System.getProperty("UpdateByControl.maximumStaticMemoryOverhead", "1.1"));
    }

    public static int initialHashTableSizeDefault() {
        return Integer.getInteger("UpdateByControl.initialHashTableSize", 4096);
    }

    public static double maximumLoadFactorDefault() {
        return Double.parseDouble(System.getProperty("UpdateByControl.maximumLoadFactor", "0.75"));
    }

    public static double targetLoadFactorDefault() {
        return Double.parseDouble(System.getProperty("UpdateByControl.targetLoadFactor", "0.7"));
    }

    public static MathContext mathContextDefault() {
        final String p = System.getProperty("UpdateByControl.mathContext", "DECIMAL64");
        switch (p) {
            case "UNLIMITED":
                return MathContext.UNLIMITED;
            case "DECIMAL32":
                return MathContext.DECIMAL32;
            case "DECIMAL64":
                return MathContext.DECIMAL64;
            case "DECIMAL128":
                return MathContext.DECIMAL128;
            default:
                throw new IllegalArgumentException("Unexpected UpdateByControl.mathContext: " + p);
        }
    }

    @Nullable
    public abstract Boolean useRedirection();

    public abstract OptionalInt chunkCapacity();

    public abstract OptionalDouble maxStaticSparseMemoryOverhead();

    public abstract OptionalInt initialHashTableSize();

    public abstract OptionalDouble maximumLoadFactor();

    public abstract OptionalDouble targetLoadFactor();

    public abstract Optional<MathContext> mathContext();

    /**
     * If redirections should be used for output sources instead of sparse array sources.
     *
     * <p>
     * Default is `false`. Can be changed with system property {@code UpdateByControl.useRedirection} or by providing a
     * value to {@link Builder#useRedirection(boolean)}.
     *
     * @return true if redirections should be used.
     */
    @Value.Derived
    public boolean useRedirectionOrDefault() {
        final Boolean useRedirection = useRedirection();
        return useRedirection == null ? useRedirectionDefault() : useRedirection;
    }

    /**
     * Get the maximum chunk capacity.
     *
     * <p>
     * Default is `4096`. Can be changed with system property {@code UpdateByControl.chunkCapacity} or by providing a
     * value to {@link Builder#chunkCapacity(int)}.
     *
     * @return the maximum chunk capacity.
     */
    @Value.Derived
    public int chunkCapacityOrDefault() {
        return chunkCapacity().orElseGet(UpdateByControl::chunkCapacityDefault);
    }

    /**
     * The maximum fractional memory overhead allowable for sparse redirections as a fraction (e.g. 1.1 is 10%
     * overhead). Values less than zero disable overhead checking, and result in always using the sparse structure. A
     * value of zero results in never using the sparse structure.
     *
     * <p>
     * Default is `1.1`. Can be changed with system property {@code UpdateByControl.maximumStaticMemoryOverhead} or by
     * providing a value to {@link Builder#maxStaticSparseMemoryOverhead(double)}.
     *
     * @return the maximum fractional memory overhead.
     */
    @Value.Derived
    public double maxStaticSparseMemoryOverheadOrDefault() {
        return maxStaticSparseMemoryOverhead().orElseGet(UpdateByControl::maximumStaticMemoryOverheadDefault);
    }

    /**
     * Get the initial hash table size
     *
     * <p>
     * Default is `4096`. Can be changed with system property {@code UpdateByControl.initialHashTableSize} or by
     * providing a value to {@link Builder#initialHashTableSize(int)}.
     *
     * @return the initial hash table size
     */
    @Value.Derived
    public int initialHashTableSizeOrDefault() {
        return initialHashTableSize().orElseGet(UpdateByControl::initialHashTableSizeDefault);
    }

    /**
     * Get the maximum load factor for the hash table.
     *
     * <p>
     * Default is `0.75`. Can be changed with system property {@code UpdateByControl.maximumLoadFactor} or by providing
     * a value to {@link Builder#maximumLoadFactor(double)}.
     *
     * @return the maximum load factor
     */
    @Value.Derived
    public double maximumLoadFactorOrDefault() {
        return maximumLoadFactor().orElseGet(UpdateByControl::maximumLoadFactorDefault);
    }

    /**
     * Get the target load factor for the hash table.
     *
     * <p>
     * Default is `0.7`. Can be changed with system property {@code UpdateByControl.targetLoadFactor} or by providing a
     * value to {@link Builder#targetLoadFactor(double)}.
     *
     * @return the target load factor
     */
    @Value.Derived
    public double targetLoadFactorOrDefault() {
        return targetLoadFactor().orElseGet(UpdateByControl::targetLoadFactorDefault);
    }

    @Value.Derived
    public MathContext mathContextOrDefault() {
        return mathContext().orElseGet(UpdateByControl::mathContextDefault);
    }

    public final UpdateByControl materialize() {
        return builder()
                .useRedirection(useRedirectionOrDefault())
                .chunkCapacity(chunkCapacityOrDefault())
                .maxStaticSparseMemoryOverhead(maxStaticSparseMemoryOverheadOrDefault())
                .initialHashTableSize(initialHashTableSizeOrDefault())
                .maximumLoadFactor(maximumLoadFactorOrDefault())
                .targetLoadFactor(targetLoadFactorOrDefault())
                .mathContext(mathContextOrDefault())
                .build();
    }

    @Check
    final void checkChunkCapacity() {
        if (chunkCapacityOrDefault() <= 0) {
            throw new IllegalArgumentException(String
                    .format("UpdateByControl.chunkCapacity() must be greater than 0, is %d", chunkCapacityOrDefault()));
        }
    }

    @Check
    final void checkInitialHashTableSize() {
        if (initialHashTableSizeOrDefault() <= 0) {
            throw new IllegalArgumentException(
                    String.format("UpdateByControl.initialHashTableSize() must be greater than 0, is %d",
                            initialHashTableSizeOrDefault()));
        }
    }

    @Check
    final void checkMaximumLoadFactor() {
        if (Double.isNaN(maximumLoadFactorOrDefault()) || maximumLoadFactorOrDefault() <= 0.0
                || maximumLoadFactorOrDefault() >= 1.0) {
            throw new IllegalArgumentException(
                    String.format("UpdateByControl.maximumLoadFactor() must be in the range (0.0, 1.0), is %f",
                            maximumLoadFactorOrDefault()));
        }
    }

    @Check
    final void checkTargetLoadFactor() {
        if (Double.isNaN(targetLoadFactorOrDefault()) || targetLoadFactorOrDefault() <= 0.0
                || targetLoadFactorOrDefault() >= 1.0) {
            throw new IllegalArgumentException(
                    String.format("UpdateByControl.targetLoadFactor() must be in the range (0.0, 1.0), is %f",
                            targetLoadFactorOrDefault()));
        }
    }

    @Check
    final void checkTargetLTEMaximum() {
        if (targetLoadFactorOrDefault() > maximumLoadFactorOrDefault()) {
            throw new IllegalArgumentException(String.format(
                    "UpdateByControl.targetLoadFactor() must be less than or equal to UpdateByControl.maximumLoadFactor(). targetLoadFactor=%f, maximumLoadFactor=%f",
                    targetLoadFactorOrDefault(), maximumLoadFactorOrDefault()));
        }
    }

    public interface Builder {
        Builder useRedirection(Boolean useRedirection);

        Builder chunkCapacity(int chunkCapacity);

        Builder maxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

        Builder initialHashTableSize(int initialHashTableSize);

        Builder maximumLoadFactor(double maximumLoadFactor);

        Builder targetLoadFactor(double targetLoadFactor);

        Builder mathContext(MathContext mathContext);

        UpdateByControl build();
    }
}
