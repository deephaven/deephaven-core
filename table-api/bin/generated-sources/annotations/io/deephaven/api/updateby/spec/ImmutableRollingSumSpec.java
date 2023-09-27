package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingSumSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingSumSpec.builder()}.
 */
@Generated(from = "RollingSumSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingSumSpec extends RollingSumSpec {

  private ImmutableRollingSumSpec(ImmutableRollingSumSpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingSumSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingSumSpec
        && equalTo(0, (ImmutableRollingSumSpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableRollingSumSpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -708100267;
  }

  /**
   * Prints the immutable value {@code RollingSumSpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingSumSpec{}";
  }

  /**
   * Creates an immutable copy of a {@link RollingSumSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingSumSpec instance
   */
  public static ImmutableRollingSumSpec copyOf(RollingSumSpec instance) {
    if (instance instanceof ImmutableRollingSumSpec) {
      return (ImmutableRollingSumSpec) instance;
    }
    return ImmutableRollingSumSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingSumSpec ImmutableRollingSumSpec}.
   * <pre>
   * ImmutableRollingSumSpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableRollingSumSpec builder
   */
  public static ImmutableRollingSumSpec.Builder builder() {
    return new ImmutableRollingSumSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingSumSpec ImmutableRollingSumSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingSumSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingSumSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingSumSpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingSumSpec ImmutableRollingSumSpec}.
     * @return An immutable instance of RollingSumSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingSumSpec build() {
      return new ImmutableRollingSumSpec(this);
    }
  }
}
