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
 * Immutable implementation of {@link AggSpecDistinct}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecDistinct.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecDistinct.of()}.
 */
@Generated(from = "AggSpecDistinct", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecDistinct extends AggSpecDistinct {
  private final boolean includeNulls;

  private ImmutableAggSpecDistinct(boolean includeNulls) {
    this.includeNulls = includeNulls;
  }

  /**
   * Whether {@code null} input values should be included in the distinct output values.
   * @return Whether to include nulls
   */
  @Override
  public boolean includeNulls() {
    return includeNulls;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecDistinct#includeNulls() includeNulls} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for includeNulls
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecDistinct withIncludeNulls(boolean value) {
    if (this.includeNulls == value) return this;
    return new ImmutableAggSpecDistinct(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecDistinct} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecDistinct
        && equalTo(0, (ImmutableAggSpecDistinct) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecDistinct another) {
    return includeNulls == another.includeNulls;
  }

  /**
   * Computes a hash code from attributes: {@code includeNulls}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(includeNulls);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecDistinct} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecDistinct{"
        + "includeNulls=" + includeNulls
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecDistinct} instance.
   * @param includeNulls The value for the {@code includeNulls} attribute
   * @return An immutable AggSpecDistinct instance
   */
  public static ImmutableAggSpecDistinct of(boolean includeNulls) {
    return new ImmutableAggSpecDistinct(includeNulls);
  }

  /**
   * Creates an immutable copy of a {@link AggSpecDistinct} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecDistinct instance
   */
  public static ImmutableAggSpecDistinct copyOf(AggSpecDistinct instance) {
    if (instance instanceof ImmutableAggSpecDistinct) {
      return (ImmutableAggSpecDistinct) instance;
    }
    return ImmutableAggSpecDistinct.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecDistinct ImmutableAggSpecDistinct}.
   * <pre>
   * ImmutableAggSpecDistinct.builder()
   *    .includeNulls(boolean) // required {@link AggSpecDistinct#includeNulls() includeNulls}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecDistinct builder
   */
  public static ImmutableAggSpecDistinct.Builder builder() {
    return new ImmutableAggSpecDistinct.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecDistinct ImmutableAggSpecDistinct}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecDistinct", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_INCLUDE_NULLS = 0x1L;
    private long initBits = 0x1L;

    private boolean includeNulls;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecDistinct} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecDistinct instance) {
      Objects.requireNonNull(instance, "instance");
      includeNulls(instance.includeNulls());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecDistinct#includeNulls() includeNulls} attribute.
     * @param includeNulls The value for includeNulls 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder includeNulls(boolean includeNulls) {
      this.includeNulls = includeNulls;
      initBits &= ~INIT_BIT_INCLUDE_NULLS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecDistinct ImmutableAggSpecDistinct}.
     * @return An immutable instance of AggSpecDistinct
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecDistinct build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAggSpecDistinct(includeNulls);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_INCLUDE_NULLS) != 0) attributes.add("includeNulls");
      return "Cannot build AggSpecDistinct, some of required attributes are not set " + attributes;
    }
  }
}
