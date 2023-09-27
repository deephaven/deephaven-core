package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingGroupSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingGroupSpec.builder()}.
 */
@Generated(from = "RollingGroupSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingGroupSpec extends RollingGroupSpec {

  private ImmutableRollingGroupSpec(ImmutableRollingGroupSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingGroupSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingGroupSpec
        && equalTo(0, (ImmutableRollingGroupSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingGroupSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 149477769;
  }

  /**
   * Prints the immutable value {@code RollingGroupSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingGroupSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingGroupSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingGroupSpec instance
   */
  public static ImmutableRollingGroupSpec copyOf(RollingGroupSpec instance) {
    if (instance instanceof ImmutableRollingGroupSpec) {
      return (ImmutableRollingGroupSpec) instance;
    }
    return ImmutableRollingGroupSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingGroupSpec ImmutableRollingGroupSpec}.
   * <pre>
   * ImmutableRollingGroupSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingGroupSpec builder
   */
  public static ImmutableRollingGroupSpec.Builder builder() {
    return new ImmutableRollingGroupSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingGroupSpec ImmutableRollingGroupSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingGroupSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingGroupSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingGroupSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingGroupSpec ImmutableRollingGroupSpec}.
     * @return An immutable instance of RollingGroupSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingGroupSpec build() {
      return new ImmutableRollingGroupSpec(this);
    }
  }
}
