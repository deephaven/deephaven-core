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
 * Immutable implementation of {@link AggSpecMedian}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecMedian.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecMedian.of()}.
 */
@Generated(from = "AggSpecMedian", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecMedian extends AggSpecMedian {
  private final boolean averageEvenlyDivided;

  private ImmutableAggSpecMedian(boolean averageEvenlyDivided) {
    this.averageEvenlyDivided = averageEvenlyDivided;
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
   * Copy the current immutable object by setting a value for the {@link AggSpecMedian#averageEvenlyDivided() averageEvenlyDivided} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for averageEvenlyDivided
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecMedian withAverageEvenlyDivided(boolean value) {
    if (this.averageEvenlyDivided == value) return this;
    return new ImmutableAggSpecMedian(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecMedian} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecMedian
        && equalTo(0, (ImmutableAggSpecMedian) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecMedian another) {
    return averageEvenlyDivided == another.averageEvenlyDivided;
  }

  /**
   * Computes a hash code from attributes: {@code averageEvenlyDivided}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(averageEvenlyDivided);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecMedian} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecMedian{"
        + "averageEvenlyDivided=" + averageEvenlyDivided
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecMedian} instance.
   * @param averageEvenlyDivided The value for the {@code averageEvenlyDivided} attribute
   * @return An immutable AggSpecMedian instance
   */
  public static ImmutableAggSpecMedian of(boolean averageEvenlyDivided) {
    return new ImmutableAggSpecMedian(averageEvenlyDivided);
  }

  /**
   * Creates an immutable copy of a {@link AggSpecMedian} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecMedian instance
   */
  public static ImmutableAggSpecMedian copyOf(AggSpecMedian instance) {
    if (instance instanceof ImmutableAggSpecMedian) {
      return (ImmutableAggSpecMedian) instance;
    }
    return ImmutableAggSpecMedian.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecMedian ImmutableAggSpecMedian}.
   * <pre>
   * ImmutableAggSpecMedian.builder()
   *    .averageEvenlyDivided(boolean) // required {@link AggSpecMedian#averageEvenlyDivided() averageEvenlyDivided}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecMedian builder
   */
  public static ImmutableAggSpecMedian.Builder builder() {
    return new ImmutableAggSpecMedian.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecMedian ImmutableAggSpecMedian}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecMedian", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_AVERAGE_EVENLY_DIVIDED = 0x1L;
    private long initBits = 0x1L;

    private boolean averageEvenlyDivided;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecMedian} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecMedian instance) {
      Objects.requireNonNull(instance, "instance");
      averageEvenlyDivided(instance.averageEvenlyDivided());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecMedian#averageEvenlyDivided() averageEvenlyDivided} attribute.
     * @param averageEvenlyDivided The value for averageEvenlyDivided 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder averageEvenlyDivided(boolean averageEvenlyDivided) {
      this.averageEvenlyDivided = averageEvenlyDivided;
      initBits &= ~INIT_BIT_AVERAGE_EVENLY_DIVIDED;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecMedian ImmutableAggSpecMedian}.
     * @return An immutable instance of AggSpecMedian
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecMedian build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAggSpecMedian(averageEvenlyDivided);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_AVERAGE_EVENLY_DIVIDED) != 0) attributes.add("averageEvenlyDivided");
      return "Cannot build AggSpecMedian, some of required attributes are not set " + attributes;
    }
  }
}
