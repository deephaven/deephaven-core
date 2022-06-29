package io.deephaven.api.updateBy;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Default;

import java.math.MathContext;

/**
 * An interface to control the behavior of an {@code Table#updateBy}
 */
@Immutable
@BuildableStyle
public interface UpdateByControl {
    static Builder builder() {
        return ImmutableUpdateByControl.builder();
    }

    UpdateByControl DEFAULT = ImmutableUpdateByControl.builder().build();

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
    default boolean useRedirection() {
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
    default int chunkCapacity() {
        return Integer.getInteger("UpdateByControl.chunkCapacity", 4096);
    }

    /**
     * The maximum fractional memory overhead allowable for sparse redirections.
     *
     * <p>
     * Default is `1.1`. Can be changed with system property {@code UpdateByControl.maximumStaticMemoryOverhead} or by
     * providing a value to {@code builder().maxStaticSparseMemoryOverhead}
     *
     * @return the maximum fractional memory overhead.
     */
    @Default
    default double maxStaticSparseMemoryOverhead() {
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
    default int initialHashTableSize() {
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
    default double maximumLoadFactor() {
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
    default double targetLoadFactor() {
        return Double.parseDouble(System.getProperty("UpdateByControl.targetLoadFactor", "0.75"));
    }

    @Default
    default MathContext getDefaultMathContext() {
        return MathContext.DECIMAL64;
    }

    public interface Builder {
        Builder useRedirection(boolean useRedirection);

        Builder chunkCapacity(int chunkCapacity);

        Builder maxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

        Builder initialHashTableSize(int initialHashTableSize);

        Builder maximumLoadFactor(double maximumLoadFactor);

        Builder targetLoadFactor(double targetLoadFactor);

        UpdateByControl build();
    }
}
