package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecStd}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecStd.builder()}.
 */
@Generated(from = "AggSpecStd", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecStd extends AggSpecStd {

  private ImmutableAggSpecStd(ImmutableAggSpecStd.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecStd} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecStd
        && equalTo(0, (ImmutableAggSpecStd) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecStd another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493343132;
  }

  /**
   * Prints the immutable value {@code AggSpecStd}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecStd{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecStd} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecStd instance
   */
  public static ImmutableAggSpecStd copyOf(AggSpecStd instance) {
    if (instance instanceof ImmutableAggSpecStd) {
      return (ImmutableAggSpecStd) instance;
    }
    return ImmutableAggSpecStd.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecStd ImmutableAggSpecStd}.
   * <pre>
   * ImmutableAggSpecStd.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecStd builder
   */
  public static ImmutableAggSpecStd.Builder builder() {
    return new ImmutableAggSpecStd.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecStd ImmutableAggSpecStd}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecStd", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecStd} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecStd instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecStd ImmutableAggSpecStd}.
     * @return An immutable instance of AggSpecStd
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecStd build() {
      return new ImmutableAggSpecStd(this);
    }
  }
}
