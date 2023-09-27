package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecSum}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecSum.builder()}.
 */
@Generated(from = "AggSpecSum", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecSum extends AggSpecSum {

  private ImmutableAggSpecSum(ImmutableAggSpecSum.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecSum} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecSum
        && equalTo(0, (ImmutableAggSpecSum) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecSum another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493343172;
  }

  /**
   * Prints the immutable value {@code AggSpecSum}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecSum{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecSum} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecSum instance
   */
  public static ImmutableAggSpecSum copyOf(AggSpecSum instance) {
    if (instance instanceof ImmutableAggSpecSum) {
      return (ImmutableAggSpecSum) instance;
    }
    return ImmutableAggSpecSum.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecSum ImmutableAggSpecSum}.
   * <pre>
   * ImmutableAggSpecSum.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecSum builder
   */
  public static ImmutableAggSpecSum.Builder builder() {
    return new ImmutableAggSpecSum.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecSum ImmutableAggSpecSum}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecSum", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecSum} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecSum instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecSum ImmutableAggSpecSum}.
     * @return An immutable instance of AggSpecSum
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecSum build() {
      return new ImmutableAggSpecSum(this);
    }
  }
}
