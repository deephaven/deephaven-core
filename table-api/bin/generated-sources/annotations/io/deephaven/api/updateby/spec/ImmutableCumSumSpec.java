package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CumSumSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCumSumSpec.builder()}.
 */
@Generated(from = "CumSumSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableCumSumSpec extends CumSumSpec {

  private ImmutableCumSumSpec(ImmutableCumSumSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCumSumSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCumSumSpec
        && equalTo(0, (ImmutableCumSumSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCumSumSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 608663839;
  }

  /**
   * Prints the immutable value {@code CumSumSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CumSumSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link CumSumSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CumSumSpec instance
   */
  public static ImmutableCumSumSpec copyOf(CumSumSpec instance) {
    if (instance instanceof ImmutableCumSumSpec) {
      return (ImmutableCumSumSpec) instance;
    }
    return ImmutableCumSumSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCumSumSpec ImmutableCumSumSpec}.
   * <pre>
   * ImmutableCumSumSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCumSumSpec builder
   */
  public static ImmutableCumSumSpec.Builder builder() {
    return new ImmutableCumSumSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCumSumSpec ImmutableCumSumSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CumSumSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CumSumSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CumSumSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCumSumSpec ImmutableCumSumSpec}.
     * @return An immutable instance of CumSumSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCumSumSpec build() {
      return new ImmutableCumSumSpec(this);
    }
  }
}
