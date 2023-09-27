package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingStdSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingStdSpec.builder()}.
 */
@Generated(from = "RollingStdSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingStdSpec extends RollingStdSpec {

  private ImmutableRollingStdSpec(ImmutableRollingStdSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingStdSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingStdSpec
        && equalTo(0, (ImmutableRollingStdSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingStdSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -745041107;
  }

  /**
   * Prints the immutable value {@code RollingStdSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingStdSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingStdSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingStdSpec instance
   */
  public static ImmutableRollingStdSpec copyOf(RollingStdSpec instance) {
    if (instance instanceof ImmutableRollingStdSpec) {
      return (ImmutableRollingStdSpec) instance;
    }
    return ImmutableRollingStdSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingStdSpec ImmutableRollingStdSpec}.
   * <pre>
   * ImmutableRollingStdSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingStdSpec builder
   */
  public static ImmutableRollingStdSpec.Builder builder() {
    return new ImmutableRollingStdSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingStdSpec ImmutableRollingStdSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingStdSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingStdSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingStdSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingStdSpec ImmutableRollingStdSpec}.
     * @return An immutable instance of RollingStdSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingStdSpec build() {
      return new ImmutableRollingStdSpec(this);
    }
  }
}
