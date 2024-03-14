//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

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

    public static final String USE_REDIRECTION_PROPERTY = "UpdateByControl.useRedirection";
    public static final String CHUNK_CAPACITY_PROPERTY = "UpdateByControl.chunkCapacity";
    public static final String MAXIMUM_STATIC_MEMORY_OVERHEAD_PROPERTY = "UpdateByControl.maximumStaticMemoryOverhead";
    public static final String INITIAL_HASH_TABLE_SIZE_PROPERTY = "UpdateByControl.initialHashTableSize";
    public static final String MAXIMUM_LOAD_FACTOR_PROPERTY = "UpdateByControl.maximumLoadFactor";
    public static final String TARGET_LOAD_FACTOR_PROPERTY = "UpdateByControl.targetLoadFactor";
    public static final String MATH_CONTEXT_PROPERTY = "UpdateByControl.mathContext";

    public static Builder builder() {
        return ImmutableUpdateByControl.builder();
    }

    /**
     * Creates an instance with none of the values explicitly set. Equivalent to {@code builder().build()}.
     *
     * @return the default instance
     */
    public static UpdateByControl defaultInstance() {
        return builder().build();
    }

    /**
     * Default is {@code false}. Can be changed with system property {@value USE_REDIRECTION_PROPERTY}.
     */
    public static boolean useRedirectionDefault() {
        return Boolean.getBoolean(USE_REDIRECTION_PROPERTY);
    }

    /**
     * Default is {@code 4096}. Can be changed with system property {@value CHUNK_CAPACITY_PROPERTY}.
     */
    public static int chunkCapacityDefault() {
        return Integer.getInteger(CHUNK_CAPACITY_PROPERTY, 4096);
    }

    /**
     * Default is {@code 1.1}. Can be changed with system property {@value MAXIMUM_STATIC_MEMORY_OVERHEAD_PROPERTY}.
     */
    public static double maximumStaticMemoryOverheadDefault() {
        return Double.parseDouble(System.getProperty(MAXIMUM_STATIC_MEMORY_OVERHEAD_PROPERTY, "1.1"));
    }

    /**
     * Default is {@code 4096}. Can be changed with system property {@value INITIAL_HASH_TABLE_SIZE_PROPERTY}.
     */
    public static int initialHashTableSizeDefault() {
        return Integer.getInteger(INITIAL_HASH_TABLE_SIZE_PROPERTY, 4096);
    }

    /**
     * Default is {@code 0.75}. Can be changed with system property {@value MAXIMUM_LOAD_FACTOR_PROPERTY}.
     */
    public static double maximumLoadFactorDefault() {
        return Double.parseDouble(System.getProperty(MAXIMUM_LOAD_FACTOR_PROPERTY, "0.75"));
    }

    /**
     * Default is {@code 0.7}. Can be changed with system property {@value TARGET_LOAD_FACTOR_PROPERTY}.
     */
    public static double targetLoadFactorDefault() {
        return Double.parseDouble(System.getProperty(TARGET_LOAD_FACTOR_PROPERTY, "0.7"));
    }

    /**
     * Default is {@link MathContext#DECIMAL64 DECIMAL64}. Can be changed with system property
     * {@value MATH_CONTEXT_PROPERTY}.
     */
    public static MathContext mathContextDefault() {
        final String p = System.getProperty(MATH_CONTEXT_PROPERTY, "DECIMAL64");
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
                throw new IllegalArgumentException(String.format("Unexpected '%s': %s", MATH_CONTEXT_PROPERTY, p));
        }
    }

    /**
     * If redirections should be used for output sources instead of sparse array sources.
     */
    @Nullable
    public abstract Boolean useRedirection();

    /**
     * The maximum chunk capacity.
     */
    public abstract OptionalInt chunkCapacity();

    /**
     * The maximum fractional memory overhead allowable for sparse redirections as a fraction (e.g. 1.1 is 10%
     * overhead). Values less than zero disable overhead checking, and result in always using the sparse structure. A
     * value of zero results in never using the sparse structure.
     */
    public abstract OptionalDouble maxStaticSparseMemoryOverhead();

    /**
     * The initial hash table size.
     */
    public abstract OptionalInt initialHashTableSize();

    /**
     * The maximum load factor for the hash table.
     */
    public abstract OptionalDouble maximumLoadFactor();

    /**
     * The target load factor for the hash table.
     */
    public abstract OptionalDouble targetLoadFactor();

    /**
     * The math context.
     */
    public abstract Optional<MathContext> mathContext();

    /**
     * Equivalent to {@code useRedirection() == null ? useRedirectionDefault() : useRedirection()}.
     *
     * @see #useRedirectionDefault()
     */
    @Value.Derived
    public boolean useRedirectionOrDefault() {
        final Boolean useRedirection = useRedirection();
        return useRedirection == null ? useRedirectionDefault() : useRedirection;
    }

    /**
     * Equivalent to {@code chunkCapacity().orElseGet(UpdateByControl::chunkCapacityDefault)}.
     *
     * @see #chunkCapacityDefault()
     */
    @Value.Derived
    public int chunkCapacityOrDefault() {
        return chunkCapacity().orElseGet(UpdateByControl::chunkCapacityDefault);
    }

    /**
     * Equivalent to
     * {@code maxStaticSparseMemoryOverhead().orElseGet(UpdateByControl::maximumStaticMemoryOverheadDefault)}.
     *
     * @see #maximumStaticMemoryOverheadDefault()
     */
    @Value.Derived
    public double maxStaticSparseMemoryOverheadOrDefault() {
        return maxStaticSparseMemoryOverhead().orElseGet(UpdateByControl::maximumStaticMemoryOverheadDefault);
    }

    /**
     * Equivalent to {@code initialHashTableSize().orElseGet(UpdateByControl::initialHashTableSizeDefault)}.
     *
     * @see #initialHashTableSizeDefault()
     */
    @Value.Derived
    public int initialHashTableSizeOrDefault() {
        return initialHashTableSize().orElseGet(UpdateByControl::initialHashTableSizeDefault);
    }

    /**
     * Equivalent to {@code maximumLoadFactor().orElseGet(UpdateByControl::maximumLoadFactorDefault)}.
     *
     * @see #maximumLoadFactorDefault()
     */
    @Value.Derived
    public double maximumLoadFactorOrDefault() {
        return maximumLoadFactor().orElseGet(UpdateByControl::maximumLoadFactorDefault);
    }

    /**
     * Equivalent to {@code targetLoadFactor().orElseGet(UpdateByControl::targetLoadFactorDefault)}.
     *
     * @see #targetLoadFactorDefault()
     */
    @Value.Derived
    public double targetLoadFactorOrDefault() {
        return targetLoadFactor().orElseGet(UpdateByControl::targetLoadFactorDefault);
    }

    /**
     * Equivalent to {@code mathContext().orElseGet(UpdateByControl::mathContextDefault)}.
     *
     * @see #mathContextDefault()
     */
    @Value.Derived
    public MathContext mathContextOrDefault() {
        return mathContext().orElseGet(UpdateByControl::mathContextDefault);
    }

    /**
     * Create a new instance with all of the explicit-or-default values from {@code this}. This may be useful from the
     * context of a client who wants to use client-side configuration defaults instead of server-side configuration
     * defaults.
     *
     * @return the explicit new instance
     */
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
