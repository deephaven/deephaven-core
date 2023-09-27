package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingAvgSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingAvgSpec.builder()}.
 */
@Generated(from = "RollingAvgSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingAvgSpec extends RollingAvgSpec {

  private ImmutableRollingAvgSpec(ImmutableRollingAvgSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingAvgSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingAvgSpec
        && equalTo(0, (ImmutableRollingAvgSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingAvgSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 519790684;
  }

  /**
   * Prints the immutable value {@code RollingAvgSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingAvgSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingAvgSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingAvgSpec instance
   */
  public static ImmutableRollingAvgSpec copyOf(RollingAvgSpec instance) {
    if (instance instanceof ImmutableRollingAvgSpec) {
      return (ImmutableRollingAvgSpec) instance;
    }
    return ImmutableRollingAvgSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
   * <pre>
   * ImmutableRollingAvgSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingAvgSpec builder
   */
  public static ImmutableRollingAvgSpec.Builder builder() {
    return new ImmutableRollingAvgSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingAvgSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingAvgSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingAvgSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingAvgSpec ImmutableRollingAvgSpec}.
     * @return An immutable instance of RollingAvgSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingAvgSpec build() {
      return new ImmutableRollingAvgSpec(this);
    }
  }
}
