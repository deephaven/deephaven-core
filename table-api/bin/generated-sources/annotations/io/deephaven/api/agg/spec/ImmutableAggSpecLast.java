package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecLast}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecLast.builder()}.
 */
@Generated(from = "AggSpecLast", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecLast extends AggSpecLast {

  private ImmutableAggSpecLast(ImmutableAggSpecLast.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecLast} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecLast
        && equalTo(0, (ImmutableAggSpecLast) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecLast another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1886459237;
  }

  /**
   * Prints the immutable value {@code AggSpecLast}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecLast{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecLast} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecLast instance
   */
  public static ImmutableAggSpecLast copyOf(AggSpecLast instance) {
    if (instance instanceof ImmutableAggSpecLast) {
      return (ImmutableAggSpecLast) instance;
    }
    return ImmutableAggSpecLast.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecLast ImmutableAggSpecLast}.
   * <pre>
   * ImmutableAggSpecLast.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecLast builder
   */
  public static ImmutableAggSpecLast.Builder builder() {
    return new ImmutableAggSpecLast.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecLast ImmutableAggSpecLast}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecLast", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecLast} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecLast instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecLast ImmutableAggSpecLast}.
     * @return An immutable instance of AggSpecLast
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecLast build() {
      return new ImmutableAggSpecLast(this);
    }
  }
}
