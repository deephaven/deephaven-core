package io.deephaven.api.updateby;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import java.math.MathContext;

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
     * get an instance of UpdateByControl with the defaults from the system properties applied
     *
     * @return default UpdateByControl
     */
    public static UpdateByControl defaultInstance() {
        return builder().build();
    }

    /**
     * if redirections should be used for output sources instead of sparse array sources.
     *
     * <p>
     * Default is `false`. Can be changed with system property {@code UpdateByControl.useRedirection} or by providing a
     * value to {@code builder().useRedirection}
     *
     * @return true if redirections should be used.
     */
    @Default
    public boolean useRedirection() {
        return Boolean.getBoolean("UpdateByControl.useRedirectionOutput");
    }

    /**
     * Get the default maximum chunk capacity.
     *
     * <p>
     * Default is `4096`. Can be changed with system property {@code UpdateByControl.chunkCapacity} or by providing a
     * value to {@code builder().chunkCapacity}
     *
     * @return the maximum chunk capacity.
     */
    @Default
    public int chunkCapacity() {
        return Integer.getInteger("UpdateByControl.chunkCapacity", 4096);
    }

    /**
     * The maximum fractional memory overhead allowable for sparse redirections as a fraction (e.g. 1.1 is 10%
     * overhead). Values less than zero disable overhead checking, and result in always using the sparse structure. A
     * value of zero results in never using the sparse structure.
     *
     * <p>
     * Default is `1.1`. Can be changed with system property {@code UpdateByControl.maximumStaticMemoryOverhead} or by
     * providing a value to {@code builder().maxStaticSparseMemoryOverhead}
     *
     * @return the maximum fractional memory overhead.
     */
    @Default
    public double maxStaticSparseMemoryOverhead() {
        return Double.parseDouble(System.getProperty("UpdateByControl.maximumStaticMemoryOverhead", "1.1"));
    }

    /**
     * Get the initial hash table size
     *
     * <p>
     * Default is `4096`. Can be changed with system property {@code UpdateByControl.initialHashTableSize} or by
     * providing a value to {@code builder().initialHashTableSize}
     *
     * @return the initial hash table size
     */
    @Default
    public int initialHashTableSize() {
        return Integer.getInteger("UpdateByControl.initialHashTableSize", 4096);
    }

    /**
     * Get the maximum load factor for the hash table.
     *
     * <p>
     * Default is `0.75`. Can be changed with system property {@code UpdateByControl.maximumLoadFactor} or by providing
     * a value to {@code builder().maximumLoadFactor}
     *
     * @return the maximum load factor
     */
    @Default
    public double maximumLoadFactor() {
        return Double.parseDouble(System.getProperty("UpdateByControl.maximumLoadFactor", "0.75"));
    }

    /**
     * Get the target load factor for the hash table.
     *
     * <p>
     * Default is `0.75`. Can be changed with system property {@code UpdateByControl.targetLoadFactor} or by providing a
     * value to {@code builder().targetLoadFactor}
     *
     * @return the target load factor
     */
    @Default
    public double targetLoadFactor() {
        return Double.parseDouble(System.getProperty("UpdateByControl.targetLoadFactor", "0.75"));
    }

    @Default
    public MathContext mathContext() {
        return MathContext.DECIMAL64;
    }

    @Check
    final void checkChunkCapacity() {
        if (chunkCapacity() <= 0) {
            throw new IllegalArgumentException(
                    "UpdateByControl.chunkCapacity() must be greater than 0");
        }
    }

    @Check
    final void checkInitialHashTableSize() {
        if (initialHashTableSize() <= 0) {
            throw new IllegalArgumentException(
                    "UpdateByControl.initialHashTableSize() must be greater than 0");
        }
    }

    @Check
    final void checkMaximumLoadFactor() {
        if (maximumLoadFactor() <= 0.0 || maximumLoadFactor() >= 1.0) {
            throw new IllegalArgumentException(
                    "UpdateByControl.maximumLoadFactor() must be in the range (0.0, 1.0)");
        }
    }

    @Check
    final void checkTargetLoadFactor() {
        if (targetLoadFactor() <= 0.0 || targetLoadFactor() >= 1.0) {
            throw new IllegalArgumentException(
                    "UpdateByControl.targetLoadFactor() must be in the range (0.0, 1.0)");
        }
    }

    public interface Builder {
        Builder useRedirection(boolean useRedirection);

        Builder chunkCapacity(int chunkCapacity);

        Builder maxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

        Builder initialHashTableSize(int initialHashTableSize);

        Builder maximumLoadFactor(double maximumLoadFactor);

        Builder targetLoadFactor(double targetLoadFactor);

        Builder mathContext(MathContext mathContext);

        UpdateByControl build();
    }
}
