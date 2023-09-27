package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingProductSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingProductSpec.builder()}.
 */
@Generated(from = "RollingProductSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingProductSpec extends RollingProductSpec {

  private ImmutableRollingProductSpec(ImmutableRollingProductSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingProductSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingProductSpec
        && equalTo(0, (ImmutableRollingProductSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingProductSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -332688391;
  }

  /**
   * Prints the immutable value {@code RollingProductSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingProductSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingProductSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingProductSpec instance
   */
  public static ImmutableRollingProductSpec copyOf(RollingProductSpec instance) {
    if (instance instanceof ImmutableRollingProductSpec) {
      return (ImmutableRollingProductSpec) instance;
    }
    return ImmutableRollingProductSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
   * <pre>
   * ImmutableRollingProductSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingProductSpec builder
   */
  public static ImmutableRollingProductSpec.Builder builder() {
    return new ImmutableRollingProductSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingProductSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingProductSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingProductSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingProductSpec ImmutableRollingProductSpec}.
     * @return An immutable instance of RollingProductSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingProductSpec build() {
      return new ImmutableRollingProductSpec(this);
    }
  }
}
