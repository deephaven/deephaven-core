package io.deephaven.api.agg.spec;

import io.deephaven.api.ColumnName;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecWSum}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecWSum.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecWSum.of()}.
 */
@Generated(from = "AggSpecWSum", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecWSum extends AggSpecWSum {
  private final ColumnName weight;

  private ImmutableAggSpecWSum(ColumnName weight) {
    this.weight = Objects.requireNonNull(weight, "weight");
  }

  private ImmutableAggSpecWSum(ImmutableAggSpecWSum original, ColumnName weight) {
    this.weight = weight;
  }

  /**
   * Column name for the source of input weights.
   * @return The weight column name
   */
  @Override
  public ColumnName weight() {
    return weight;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecWSum#weight() weight} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for weight
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecWSum withWeight(ColumnName value) {
    if (this.weight == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "weight");
    return new ImmutableAggSpecWSum(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecWSum} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecWSum
        && equalTo(0, (ImmutableAggSpecWSum) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecWSum another) {
    return weight.equals(another.weight);
  }

  /**
   * Computes a hash code from attributes: {@code weight}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + weight.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecWSum} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecWSum{"
        + "weight=" + weight
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecWSum} instance.
   * @param weight The value for the {@code weight} attribute
   * @return An immutable AggSpecWSum instance
   */
  public static ImmutableAggSpecWSum of(ColumnName weight) {
    return new ImmutableAggSpecWSum(weight);
  }

  /**
   * Creates an immutable copy of a {@link AggSpecWSum} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecWSum instance
   */
  public static ImmutableAggSpecWSum copyOf(AggSpecWSum instance) {
    if (instance instanceof ImmutableAggSpecWSum) {
      return (ImmutableAggSpecWSum) instance;
    }
    return ImmutableAggSpecWSum.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecWSum ImmutableAggSpecWSum}.
   * <pre>
   * ImmutableAggSpecWSum.builder()
   *    .weight(io.deephaven.api.ColumnName) // required {@link AggSpecWSum#weight() weight}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecWSum builder
   */
  public static ImmutableAggSpecWSum.Builder builder() {
    return new ImmutableAggSpecWSum.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecWSum ImmutableAggSpecWSum}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecWSum", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_WEIGHT = 0x1L;
    private long initBits = 0x1L;

    private @Nullable ColumnName weight;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecWSum} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecWSum instance) {
      Objects.requireNonNull(instance, "instance");
      weight(instance.weight());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecWSum#weight() weight} attribute.
     * @param weight The value for weight 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder weight(ColumnName weight) {
      this.weight = Objects.requireNonNull(weight, "weight");
      initBits &= ~INIT_BIT_WEIGHT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecWSum ImmutableAggSpecWSum}.
     * @return An immutable instance of AggSpecWSum
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecWSum build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAggSpecWSum(null, weight);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_WEIGHT) != 0) attributes.add("weight");
      return "Cannot build AggSpecWSum, some of required attributes are not set " + attributes;
    }
  }
}
