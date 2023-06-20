package io.deephaven.api.agg.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecPercentile}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecPercentile.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecPercentile.of()}.
 */
@Generated(from = "AggSpecPercentile", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecPercentile extends AggSpecPercentile {
  private final double percentile;
  private final boolean averageEvenlyDivided;

  private ImmutableAggSpecPercentile(double percentile, boolean averageEvenlyDivided) {
    this.percentile = percentile;
    this.averageEvenlyDivided = averageEvenlyDivided;
  }

  /**
   * The percentile to calculate. Must be &gt;= 0.0 and &lt;= 1.0.
   * @return The percentile to calculate
   */
  @Override
  public double percentile() {
    return percentile;
  }

  /**
   * Whether to average the highest low-bucket value and lowest high-bucket value, when the low-bucket and high-bucket
   * are of equal size. Only applies to numeric types.
   * @return Whether to average the two result candidates for evenly-divided input sets of numeric types
   */
  @Override
  public boolean averageEvenlyDivided() {
    return averageEvenlyDivided;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecPercentile#percentile() percentile} attribute.
   * A value strict bits equality used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for percentile
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecPercentile withPercentile(double value) {
    if (Double.doubleToLongBits(this.percentile) == Double.doubleToLongBits(value)) return this;
    return validate(new ImmutableAggSpecPercentile(value, this.averageEvenlyDivided));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecPercentile#averageEvenlyDivided() averageEvenlyDivided} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for averageEvenlyDivided
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecPercentile withAverageEvenlyDivided(boolean value) {
    if (this.averageEvenlyDivided == value) return this;
    return validate(new ImmutableAggSpecPercentile(this.percentile, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecPercentile} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecPercentile
        && equalTo(0, (ImmutableAggSpecPercentile) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecPercentile another) {
    return Double.doubleToLongBits(percentile) == Double.doubleToLongBits(another.percentile)
        && averageEvenlyDivided == another.averageEvenlyDivided;
  }

  /**
   * Computes a hash code from attributes: {@code percentile}, {@code averageEvenlyDivided}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Double.hashCode(percentile);
    h += (h << 5) + Boolean.hashCode(averageEvenlyDivided);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecPercentile} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecPercentile{"
        + "percentile=" + percentile
        + ", averageEvenlyDivided=" + averageEvenlyDivided
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecPercentile} instance.
   * @param percentile The value for the {@code percentile} attribute
   * @param averageEvenlyDivided The value for the {@code averageEvenlyDivided} attribute
   * @return An immutable AggSpecPercentile instance
   */
  public static ImmutableAggSpecPercentile of(double percentile, boolean averageEvenlyDivided) {
    return validate(new ImmutableAggSpecPercentile(percentile, averageEvenlyDivided));
  }

  private static ImmutableAggSpecPercentile validate(ImmutableAggSpecPercentile instance) {
    instance.checkPercentile();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AggSpecPercentile} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecPercentile instance
   */
  public static ImmutableAggSpecPercentile copyOf(AggSpecPercentile instance) {
    if (instance instanceof ImmutableAggSpecPercentile) {
      return (ImmutableAggSpecPercentile) instance;
    }
    return ImmutableAggSpecPercentile.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecPercentile ImmutableAggSpecPercentile}.
   * <pre>
   * ImmutableAggSpecPercentile.builder()
   *    .percentile(double) // required {@link AggSpecPercentile#percentile() percentile}
   *    .averageEvenlyDivided(boolean) // required {@link AggSpecPercentile#averageEvenlyDivided() averageEvenlyDivided}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecPercentile builder
   */
  public static ImmutableAggSpecPercentile.Builder builder() {
    return new ImmutableAggSpecPercentile.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecPercentile ImmutableAggSpecPercentile}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecPercentile", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_PERCENTILE = 0x1L;
    private static final long INIT_BIT_AVERAGE_EVENLY_DIVIDED = 0x2L;
    private long initBits = 0x3L;

    private double percentile;
    private boolean averageEvenlyDivided;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecPercentile} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecPercentile instance) {
      Objects.requireNonNull(instance, "instance");
      percentile(instance.percentile());
      averageEvenlyDivided(instance.averageEvenlyDivided());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecPercentile#percentile() percentile} attribute.
     * @param percentile The value for percentile 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder percentile(double percentile) {
      this.percentile = percentile;
      initBits &= ~INIT_BIT_PERCENTILE;
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecPercentile#averageEvenlyDivided() averageEvenlyDivided} attribute.
     * @param averageEvenlyDivided The value for averageEvenlyDivided 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder averageEvenlyDivided(boolean averageEvenlyDivided) {
      this.averageEvenlyDivided = averageEvenlyDivided;
      initBits &= ~INIT_BIT_AVERAGE_EVENLY_DIVIDED;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecPercentile ImmutableAggSpecPercentile}.
     * @return An immutable instance of AggSpecPercentile
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecPercentile build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableAggSpecPercentile.validate(new ImmutableAggSpecPercentile(percentile, averageEvenlyDivided));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PERCENTILE) != 0) attributes.add("percentile");
      if ((initBits & INIT_BIT_AVERAGE_EVENLY_DIVIDED) != 0) attributes.add("averageEvenlyDivided");
      return "Cannot build AggSpecPercentile, some of required attributes are not set " + attributes;
    }
  }
}
