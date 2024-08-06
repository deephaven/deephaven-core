package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecFreeze}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecFreeze.builder()}.
 */
@Generated(from = "AggSpecFreeze", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecFreeze extends AggSpecFreeze {

  private ImmutableAggSpecFreeze(ImmutableAggSpecFreeze.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecFreeze} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecFreeze
        && equalTo(0, (ImmutableAggSpecFreeze) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecFreeze another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -567660260;
  }

  /**
   * Prints the immutable value {@code AggSpecFreeze}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecFreeze{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecFreeze} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecFreeze instance
   */
  public static ImmutableAggSpecFreeze copyOf(AggSpecFreeze instance) {
    if (instance instanceof ImmutableAggSpecFreeze) {
      return (ImmutableAggSpecFreeze) instance;
    }
    return ImmutableAggSpecFreeze.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecFreeze ImmutableAggSpecFreeze}.
   * <pre>
   * ImmutableAggSpecFreeze.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecFreeze builder
   */
  public static ImmutableAggSpecFreeze.Builder builder() {
    return new ImmutableAggSpecFreeze.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecFreeze ImmutableAggSpecFreeze}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecFreeze", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecFreeze} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecFreeze instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecFreeze ImmutableAggSpecFreeze}.
     * @return An immutable instance of AggSpecFreeze
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecFreeze build() {
      return new ImmutableAggSpecFreeze(this);
    }
  }
}
