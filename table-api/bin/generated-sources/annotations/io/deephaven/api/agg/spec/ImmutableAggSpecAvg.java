package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecAvg}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecAvg.builder()}.
 */
@Generated(from = "AggSpecAvg", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecAvg extends AggSpecAvg {

  private ImmutableAggSpecAvg(ImmutableAggSpecAvg.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecAvg} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecAvg
        && equalTo(0, (ImmutableAggSpecAvg) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecAvg another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493325899;
  }

  /**
   * Prints the immutable value {@code AggSpecAvg}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecAvg{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecAvg} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecAvg instance
   */
  public static ImmutableAggSpecAvg copyOf(AggSpecAvg instance) {
    if (instance instanceof ImmutableAggSpecAvg) {
      return (ImmutableAggSpecAvg) instance;
    }
    return ImmutableAggSpecAvg.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecAvg ImmutableAggSpecAvg}.
   * <pre>
   * ImmutableAggSpecAvg.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecAvg builder
   */
  public static ImmutableAggSpecAvg.Builder builder() {
    return new ImmutableAggSpecAvg.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecAvg ImmutableAggSpecAvg}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecAvg", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecAvg} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecAvg instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecAvg ImmutableAggSpecAvg}.
     * @return An immutable instance of AggSpecAvg
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecAvg build() {
      return new ImmutableAggSpecAvg(this);
    }
  }
}
