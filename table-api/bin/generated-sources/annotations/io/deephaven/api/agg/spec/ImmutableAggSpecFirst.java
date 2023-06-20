package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecFirst}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecFirst.builder()}.
 */
@Generated(from = "AggSpecFirst", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecFirst extends AggSpecFirst {

  private ImmutableAggSpecFirst(ImmutableAggSpecFirst.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecFirst} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecFirst
        && equalTo(0, (ImmutableAggSpecFirst) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecFirst another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1644001193;
  }

  /**
   * Prints the immutable value {@code AggSpecFirst}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecFirst{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecFirst} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecFirst instance
   */
  public static ImmutableAggSpecFirst copyOf(AggSpecFirst instance) {
    if (instance instanceof ImmutableAggSpecFirst) {
      return (ImmutableAggSpecFirst) instance;
    }
    return ImmutableAggSpecFirst.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecFirst ImmutableAggSpecFirst}.
   * <pre>
   * ImmutableAggSpecFirst.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecFirst builder
   */
  public static ImmutableAggSpecFirst.Builder builder() {
    return new ImmutableAggSpecFirst.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecFirst ImmutableAggSpecFirst}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecFirst", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecFirst} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecFirst instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecFirst ImmutableAggSpecFirst}.
     * @return An immutable instance of AggSpecFirst
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecFirst build() {
      return new ImmutableAggSpecFirst(this);
    }
  }
}
