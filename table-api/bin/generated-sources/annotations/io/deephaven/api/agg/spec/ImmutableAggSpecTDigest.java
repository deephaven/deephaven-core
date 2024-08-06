package io.deephaven.api.agg.spec;

import java.util.Objects;
import java.util.OptionalDouble;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecTDigest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecTDigest.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecTDigest.of()}.
 */
@Generated(from = "AggSpecTDigest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecTDigest extends AggSpecTDigest {
  private final @Nullable Double compression;

  private ImmutableAggSpecTDigest(OptionalDouble compression) {
    this.compression = compression.isPresent() ? compression.getAsDouble() : null;
  }

  private ImmutableAggSpecTDigest(ImmutableAggSpecTDigest original, @Nullable Double compression) {
    this.compression = compression;
  }

  /**
   * T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large.
   * <p>
   * When not specified, the server will choose a compression value.
   * @return The T-Digest compression factor if specified
   */
  @Override
  public OptionalDouble compression() {
    return compression != null
        ? OptionalDouble.of(compression)
        : OptionalDouble.empty();
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link AggSpecTDigest#compression() compression} attribute.
   * @param value The value for compression
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecTDigest withCompression(double value) {
    @Nullable Double newValue = value;
    if (Objects.equals(this.compression, newValue)) return this;
    return validate(new ImmutableAggSpecTDigest(this, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link AggSpecTDigest#compression() compression} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for compression
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecTDigest withCompression(OptionalDouble optional) {
    @Nullable Double value = optional.isPresent() ? optional.getAsDouble() : null;
    if (Objects.equals(this.compression, value)) return this;
    return validate(new ImmutableAggSpecTDigest(this, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecTDigest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecTDigest
        && equalTo(0, (ImmutableAggSpecTDigest) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecTDigest another) {
    return Objects.equals(compression, another.compression);
  }

  /**
   * Computes a hash code from attributes: {@code compression}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(compression);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecTDigest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AggSpecTDigest{");
    if (compression != null) {
      builder.append("compression=").append(compression);
    }
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code AggSpecTDigest} instance.
   * @param compression The value for the {@code compression} attribute
   * @return An immutable AggSpecTDigest instance
   */
  public static ImmutableAggSpecTDigest of(OptionalDouble compression) {
    return validate(new ImmutableAggSpecTDigest(compression));
  }

  private static ImmutableAggSpecTDigest validate(ImmutableAggSpecTDigest instance) {
    instance.checkCompression();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AggSpecTDigest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecTDigest instance
   */
  public static ImmutableAggSpecTDigest copyOf(AggSpecTDigest instance) {
    if (instance instanceof ImmutableAggSpecTDigest) {
      return (ImmutableAggSpecTDigest) instance;
    }
    return ImmutableAggSpecTDigest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecTDigest ImmutableAggSpecTDigest}.
   * <pre>
   * ImmutableAggSpecTDigest.builder()
   *    .compression(double) // optional {@link AggSpecTDigest#compression() compression}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecTDigest builder
   */
  public static ImmutableAggSpecTDigest.Builder builder() {
    return new ImmutableAggSpecTDigest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecTDigest ImmutableAggSpecTDigest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecTDigest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private @Nullable Double compression;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecTDigest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecTDigest instance) {
      Objects.requireNonNull(instance, "instance");
      OptionalDouble compressionOptional = instance.compression();
      if (compressionOptional.isPresent()) {
        compression(compressionOptional);
      }
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecTDigest#compression() compression} to compression.
     * @param compression The value for compression
     * @return {@code this} builder for chained invocation
     */
    public final Builder compression(double compression) {
      this.compression = compression;
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecTDigest#compression() compression} to compression.
     * @param compression The value for compression
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder compression(OptionalDouble compression) {
      this.compression = compression.isPresent() ? compression.getAsDouble() : null;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecTDigest ImmutableAggSpecTDigest}.
     * @return An immutable instance of AggSpecTDigest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecTDigest build() {
      return ImmutableAggSpecTDigest.validate(new ImmutableAggSpecTDigest(null, compression));
    }
  }
}
