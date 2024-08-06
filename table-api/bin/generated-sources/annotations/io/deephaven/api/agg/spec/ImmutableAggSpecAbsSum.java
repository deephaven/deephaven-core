package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecAbsSum}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecAbsSum.builder()}.
 */
@Generated(from = "AggSpecAbsSum", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecAbsSum extends AggSpecAbsSum {

  private ImmutableAggSpecAbsSum(ImmutableAggSpecAbsSum.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecAbsSum} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecAbsSum
        && equalTo(0, (ImmutableAggSpecAbsSum) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecAbsSum another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -725182722;
  }

  /**
   * Prints the immutable value {@code AggSpecAbsSum}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecAbsSum{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecAbsSum} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecAbsSum instance
   */
  public static ImmutableAggSpecAbsSum copyOf(AggSpecAbsSum instance) {
    if (instance instanceof ImmutableAggSpecAbsSum) {
      return (ImmutableAggSpecAbsSum) instance;
    }
    return ImmutableAggSpecAbsSum.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecAbsSum ImmutableAggSpecAbsSum}.
   * <pre>
   * ImmutableAggSpecAbsSum.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecAbsSum builder
   */
  public static ImmutableAggSpecAbsSum.Builder builder() {
    return new ImmutableAggSpecAbsSum.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecAbsSum ImmutableAggSpecAbsSum}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecAbsSum", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecAbsSum} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecAbsSum instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecAbsSum ImmutableAggSpecAbsSum}.
     * @return An immutable instance of AggSpecAbsSum
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecAbsSum build() {
      return new ImmutableAggSpecAbsSum(this);
    }
  }
}
