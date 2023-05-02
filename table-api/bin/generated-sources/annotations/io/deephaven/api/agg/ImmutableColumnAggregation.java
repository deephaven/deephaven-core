package io.deephaven.api.agg;

import io.deephaven.api.agg.spec.AggSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnAggregation}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnAggregation.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnAggregation.of()}.
 */
@Generated(from = "ColumnAggregation", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableColumnAggregation extends ColumnAggregation {
  private final AggSpec spec;
  private final Pair pair;

  private ImmutableColumnAggregation(AggSpec spec, Pair pair) {
    this.spec = Objects.requireNonNull(spec, "spec");
    this.pair = Objects.requireNonNull(pair, "pair");
  }

  private ImmutableColumnAggregation(
      ImmutableColumnAggregation original,
      AggSpec spec,
      Pair pair) {
    this.spec = spec;
    this.pair = pair;
  }

  /**
   * @return The value of the {@code spec} attribute
   */
  @Override
  public AggSpec spec() {
    return spec;
  }

  /**
   * @return The value of the {@code pair} attribute
   */
  @Override
  public Pair pair() {
    return pair;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnAggregation#spec() spec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for spec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnAggregation withSpec(AggSpec value) {
    if (this.spec == value) return this;
    AggSpec newValue = Objects.requireNonNull(value, "spec");
    return new ImmutableColumnAggregation(this, newValue, this.pair);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnAggregation#pair() pair} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for pair
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnAggregation withPair(Pair value) {
    if (this.pair == value) return this;
    Pair newValue = Objects.requireNonNull(value, "pair");
    return new ImmutableColumnAggregation(this, this.spec, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnAggregation} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnAggregation
        && equalTo(0, (ImmutableColumnAggregation) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnAggregation another) {
    return spec.equals(another.spec)
        && pair.equals(another.pair);
  }

  /**
   * Computes a hash code from attributes: {@code spec}, {@code pair}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + spec.hashCode();
    h += (h << 5) + pair.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnAggregation} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnAggregation{"
        + "spec=" + spec
        + ", pair=" + pair
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnAggregation} instance.
   * @param spec The value for the {@code spec} attribute
   * @param pair The value for the {@code pair} attribute
   * @return An immutable ColumnAggregation instance
   */
  public static ImmutableColumnAggregation of(AggSpec spec, Pair pair) {
    return new ImmutableColumnAggregation(spec, pair);
  }

  /**
   * Creates an immutable copy of a {@link ColumnAggregation} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ColumnAggregation instance
   */
  public static ImmutableColumnAggregation copyOf(ColumnAggregation instance) {
    if (instance instanceof ImmutableColumnAggregation) {
      return (ImmutableColumnAggregation) instance;
    }
    return ImmutableColumnAggregation.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnAggregation ImmutableColumnAggregation}.
   * <pre>
   * ImmutableColumnAggregation.builder()
   *    .spec(io.deephaven.api.agg.spec.AggSpec) // required {@link ColumnAggregation#spec() spec}
   *    .pair(io.deephaven.api.agg.Pair) // required {@link ColumnAggregation#pair() pair}
   *    .build();
   * </pre>
   * @return A new ImmutableColumnAggregation builder
   */
  public static ImmutableColumnAggregation.Builder builder() {
    return new ImmutableColumnAggregation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableColumnAggregation ImmutableColumnAggregation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnAggregation", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_SPEC = 0x1L;
    private static final long INIT_BIT_PAIR = 0x2L;
    private long initBits = 0x3L;

    private @Nullable AggSpec spec;
    private @Nullable Pair pair;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnAggregation} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ColumnAggregation instance) {
      Objects.requireNonNull(instance, "instance");
      spec(instance.spec());
      pair(instance.pair());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnAggregation#spec() spec} attribute.
     * @param spec The value for spec 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder spec(AggSpec spec) {
      this.spec = Objects.requireNonNull(spec, "spec");
      initBits &= ~INIT_BIT_SPEC;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnAggregation#pair() pair} attribute.
     * @param pair The value for pair 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder pair(Pair pair) {
      this.pair = Objects.requireNonNull(pair, "pair");
      initBits &= ~INIT_BIT_PAIR;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnAggregation ImmutableColumnAggregation}.
     * @return An immutable instance of ColumnAggregation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnAggregation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnAggregation(null, spec, pair);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SPEC) != 0) attributes.add("spec");
      if ((initBits & INIT_BIT_PAIR) != 0) attributes.add("pair");
      return "Cannot build ColumnAggregation, some of required attributes are not set " + attributes;
    }
  }
}
