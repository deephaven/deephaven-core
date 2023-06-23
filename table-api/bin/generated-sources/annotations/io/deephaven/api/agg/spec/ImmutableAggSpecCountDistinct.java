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
 * Immutable implementation of {@link AggSpecCountDistinct}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecCountDistinct.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecCountDistinct.of()}.
 */
@Generated(from = "AggSpecCountDistinct", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecCountDistinct extends AggSpecCountDistinct {
  private final boolean countNulls;

  private ImmutableAggSpecCountDistinct(boolean countNulls) {
    this.countNulls = countNulls;
  }

  /**
   * Whether {@code null} input values should be included when counting the distinct input values.
   * 
   * @return Whether to count nulls
   */
  @Override
  public boolean countNulls() {
    return countNulls;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecCountDistinct#countNulls() countNulls} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for countNulls
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecCountDistinct withCountNulls(boolean value) {
    if (this.countNulls == value) return this;
    return new ImmutableAggSpecCountDistinct(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecCountDistinct} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecCountDistinct
        && equalTo(0, (ImmutableAggSpecCountDistinct) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecCountDistinct another) {
    return countNulls == another.countNulls;
  }

  /**
   * Computes a hash code from attributes: {@code countNulls}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(countNulls);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecCountDistinct} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecCountDistinct{"
        + "countNulls=" + countNulls
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecCountDistinct} instance.
   * @param countNulls The value for the {@code countNulls} attribute
   * @return An immutable AggSpecCountDistinct instance
   */
  public static ImmutableAggSpecCountDistinct of(boolean countNulls) {
    return new ImmutableAggSpecCountDistinct(countNulls);
  }

  /**
   * Creates an immutable copy of a {@link AggSpecCountDistinct} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecCountDistinct instance
   */
  public static ImmutableAggSpecCountDistinct copyOf(AggSpecCountDistinct instance) {
    if (instance instanceof ImmutableAggSpecCountDistinct) {
      return (ImmutableAggSpecCountDistinct) instance;
    }
    return ImmutableAggSpecCountDistinct.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecCountDistinct ImmutableAggSpecCountDistinct}.
   * <pre>
   * ImmutableAggSpecCountDistinct.builder()
   *    .countNulls(boolean) // required {@link AggSpecCountDistinct#countNulls() countNulls}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecCountDistinct builder
   */
  public static ImmutableAggSpecCountDistinct.Builder builder() {
    return new ImmutableAggSpecCountDistinct.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecCountDistinct ImmutableAggSpecCountDistinct}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecCountDistinct", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COUNT_NULLS = 0x1L;
    private long initBits = 0x1L;

    private boolean countNulls;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecCountDistinct} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecCountDistinct instance) {
      Objects.requireNonNull(instance, "instance");
      countNulls(instance.countNulls());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecCountDistinct#countNulls() countNulls} attribute.
     * @param countNulls The value for countNulls 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder countNulls(boolean countNulls) {
      this.countNulls = countNulls;
      initBits &= ~INIT_BIT_COUNT_NULLS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecCountDistinct ImmutableAggSpecCountDistinct}.
     * @return An immutable instance of AggSpecCountDistinct
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecCountDistinct build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAggSpecCountDistinct(countNulls);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COUNT_NULLS) != 0) attributes.add("countNulls");
      return "Cannot build AggSpecCountDistinct, some of required attributes are not set " + attributes;
    }
  }
}
