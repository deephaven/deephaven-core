package io.deephaven.api.agg.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecApproximatePercentile}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecApproximatePercentile.builder()}.
 */
@Generated(from = "AggSpecApproximatePercentile", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecApproximatePercentile
    extends AggSpecApproximatePercentile {
  private final double percentile;
  private final @Nullable Double compression;

  private ImmutableAggSpecApproximatePercentile(double percentile, @Nullable Double compression) {
    this.percentile = percentile;
    this.compression = compression;
  }

  /**
   * Percentile. Must be in range [0.0, 1.0].
   * @return The percentile
   */
  @Override
  public double percentile() {
    return percentile;
  }

  /**
   * T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large.
   * <p>
   * When not specified, the engine will choose a compression value.
   * @return The T-Digest compression factor if specified
   */
  @Override
  public OptionalDouble compression() {
    return compression != null
        ? OptionalDouble.of(compression)
        : OptionalDouble.empty();
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecApproximatePercentile#percentile() percentile} attribute.
   * A value strict bits equality used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for percentile
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecApproximatePercentile withPercentile(double value) {
    if (Double.doubleToLongBits(this.percentile) == Double.doubleToLongBits(value)) return this;
    return validate(new ImmutableAggSpecApproximatePercentile(value, this.compression));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link AggSpecApproximatePercentile#compression() compression} attribute.
   * @param value The value for compression
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecApproximatePercentile withCompression(double value) {
    @Nullable Double newValue = value;
    if (Objects.equals(this.compression, newValue)) return this;
    return validate(new ImmutableAggSpecApproximatePercentile(this.percentile, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link AggSpecApproximatePercentile#compression() compression} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for compression
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecApproximatePercentile withCompression(OptionalDouble optional) {
    @Nullable Double value = optional.isPresent() ? optional.getAsDouble() : null;
    if (Objects.equals(this.compression, value)) return this;
    return validate(new ImmutableAggSpecApproximatePercentile(this.percentile, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecApproximatePercentile} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecApproximatePercentile
        && equalTo(0, (ImmutableAggSpecApproximatePercentile) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecApproximatePercentile another) {
    return Double.doubleToLongBits(percentile) == Double.doubleToLongBits(another.percentile)
        && Objects.equals(compression, another.compression);
  }

  /**
   * Computes a hash code from attributes: {@code percentile}, {@code compression}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Double.hashCode(percentile);
    h += (h << 5) + Objects.hashCode(compression);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecApproximatePercentile} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AggSpecApproximatePercentile{");
    builder.append("percentile=").append(percentile);
    if (compression != null) {
      builder.append(", ");
      builder.append("compression=").append(compression);
    }
    return builder.append("}").toString();
  }

  private static ImmutableAggSpecApproximatePercentile validate(ImmutableAggSpecApproximatePercentile instance) {
    instance.checkCompression();
    instance.checkPercentile();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AggSpecApproximatePercentile} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecApproximatePercentile instance
   */
  public static ImmutableAggSpecApproximatePercentile copyOf(AggSpecApproximatePercentile instance) {
    if (instance instanceof ImmutableAggSpecApproximatePercentile) {
      return (ImmutableAggSpecApproximatePercentile) instance;
    }
    return ImmutableAggSpecApproximatePercentile.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecApproximatePercentile ImmutableAggSpecApproximatePercentile}.
   * <pre>
   * ImmutableAggSpecApproximatePercentile.builder()
   *    .percentile(double) // required {@link AggSpecApproximatePercentile#percentile() percentile}
   *    .compression(double) // optional {@link AggSpecApproximatePercentile#compression() compression}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecApproximatePercentile builder
   */
  public static ImmutableAggSpecApproximatePercentile.Builder builder() {
    return new ImmutableAggSpecApproximatePercentile.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecApproximatePercentile ImmutableAggSpecApproximatePercentile}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecApproximatePercentile", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_PERCENTILE = 0x1L;
    private long initBits = 0x1L;

    private double percentile;
    private @Nullable Double compression;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecApproximatePercentile} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecApproximatePercentile instance) {
      Objects.requireNonNull(instance, "instance");
      percentile(instance.percentile());
      OptionalDouble compressionOptional = instance.compression();
      if (compressionOptional.isPresent()) {
        compression(compressionOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecApproximatePercentile#percentile() percentile} attribute.
     * @param percentile The value for percentile 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder percentile(double percentile) {
      this.percentile = percentile;
      initBits &= ~INIT_BIT_PERCENTILE;
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecApproximatePercentile#compression() compression} to compression.
     * @param compression The value for compression
     * @return {@code this} builder for chained invocation
     */
    public final Builder compression(double compression) {
      this.compression = compression;
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecApproximatePercentile#compression() compression} to compression.
     * @param compression The value for compression
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder compression(OptionalDouble compression) {
      this.compression = compression.isPresent() ? compression.getAsDouble() : null;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecApproximatePercentile ImmutableAggSpecApproximatePercentile}.
     * @return An immutable instance of AggSpecApproximatePercentile
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecApproximatePercentile build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableAggSpecApproximatePercentile.validate(new ImmutableAggSpecApproximatePercentile(percentile, compression));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PERCENTILE) != 0) attributes.add("percentile");
      return "Cannot build AggSpecApproximatePercentile, some of required attributes are not set " + attributes;
    }
  }
}
