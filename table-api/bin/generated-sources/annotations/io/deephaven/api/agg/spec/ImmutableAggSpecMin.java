package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecMin}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecMin.builder()}.
 */
@Generated(from = "AggSpecMin", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecMin extends AggSpecMin {

  private ImmutableAggSpecMin(ImmutableAggSpecMin.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecMin} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecMin
        && equalTo(0, (ImmutableAggSpecMin) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecMin another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493337035;
  }

  /**
   * Prints the immutable value {@code AggSpecMin}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecMin{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecMin} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecMin instance
   */
  public static ImmutableAggSpecMin copyOf(AggSpecMin instance) {
    if (instance instanceof ImmutableAggSpecMin) {
      return (ImmutableAggSpecMin) instance;
    }
    return ImmutableAggSpecMin.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecMin ImmutableAggSpecMin}.
   * <pre>
   * ImmutableAggSpecMin.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecMin builder
   */
  public static ImmutableAggSpecMin.Builder builder() {
    return new ImmutableAggSpecMin.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecMin ImmutableAggSpecMin}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecMin", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecMin} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecMin instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecMin ImmutableAggSpecMin}.
     * @return An immutable instance of AggSpecMin
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecMin build() {
      return new ImmutableAggSpecMin(this);
    }
  }
}
