package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingCountSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingCountSpec.builder()}.
 */
@Generated(from = "RollingCountSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingCountSpec extends RollingCountSpec {

  private ImmutableRollingCountSpec(ImmutableRollingCountSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingCountSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingCountSpec
        && equalTo(0, (ImmutableRollingCountSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingCountSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1310757863;
  }

  /**
   * Prints the immutable value {@code RollingCountSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingCountSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingCountSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingCountSpec instance
   */
  public static ImmutableRollingCountSpec copyOf(RollingCountSpec instance) {
    if (instance instanceof ImmutableRollingCountSpec) {
      return (ImmutableRollingCountSpec) instance;
    }
    return ImmutableRollingCountSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingCountSpec ImmutableRollingCountSpec}.
   * <pre>
   * ImmutableRollingCountSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingCountSpec builder
   */
  public static ImmutableRollingCountSpec.Builder builder() {
    return new ImmutableRollingCountSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingCountSpec ImmutableRollingCountSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingCountSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingCountSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingCountSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingCountSpec ImmutableRollingCountSpec}.
     * @return An immutable instance of RollingCountSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingCountSpec build() {
      return new ImmutableRollingCountSpec(this);
    }
  }
}
