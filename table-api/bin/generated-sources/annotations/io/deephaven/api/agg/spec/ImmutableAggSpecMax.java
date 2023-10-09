package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecMax}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecMax.builder()}.
 */
@Generated(from = "AggSpecMax", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecMax extends AggSpecMax {

  private ImmutableAggSpecMax(ImmutableAggSpecMax.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecMax} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecMax
        && equalTo(0, (ImmutableAggSpecMax) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecMax another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493336797;
  }

  /**
   * Prints the immutable value {@code AggSpecMax}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecMax{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecMax} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecMax instance
   */
  public static ImmutableAggSpecMax copyOf(AggSpecMax instance) {
    if (instance instanceof ImmutableAggSpecMax) {
      return (ImmutableAggSpecMax) instance;
    }
    return ImmutableAggSpecMax.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecMax ImmutableAggSpecMax}.
   * <pre>
   * ImmutableAggSpecMax.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecMax builder
   */
  public static ImmutableAggSpecMax.Builder builder() {
    return new ImmutableAggSpecMax.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecMax ImmutableAggSpecMax}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecMax", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecMax} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecMax instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecMax ImmutableAggSpecMax}.
     * @return An immutable instance of AggSpecMax
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecMax build() {
      return new ImmutableAggSpecMax(this);
    }
  }
}
