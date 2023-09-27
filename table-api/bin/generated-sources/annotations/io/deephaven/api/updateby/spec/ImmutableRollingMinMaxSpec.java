package io.deephaven.api.updateby.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RollingMinMaxSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingMinMaxSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRollingMinMaxSpec.of()}.
 */
@Generated(from = "RollingMinMaxSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingMinMaxSpec extends RollingMinMaxSpec {
  private final boolean isMax;

  private ImmutableRollingMinMaxSpec(boolean isMax) {
    this.isMax = isMax;
  }

  /**
   * @return The value of the {@code isMax} attribute
   */
  @Override
  public boolean isMax() {
    return isMax;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingMinMaxSpec#isMax() isMax} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isMax
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingMinMaxSpec withIsMax(boolean value) {
    if (this.isMax == value) return this;
    return new ImmutableRollingMinMaxSpec(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingMinMaxSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingMinMaxSpec
        && equalTo(0, (ImmutableRollingMinMaxSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingMinMaxSpec another) {
    return isMax == another.isMax;
  }

  /**
   * Computes a hash code from attributes: {@code isMax}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(isMax);
    return h;
  }

  /**
   * Prints the immutable value {@code RollingMinMaxSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingMinMaxSpec{"
        + "isMax=" + isMax
        + "}";
  }

  /**
   * Construct a new immutable {@code RollingMinMaxSpec} instance.
   * @param isMax The value for the {@code isMax} attribute
   * @return An immutable RollingMinMaxSpec instance
   */
  public static ImmutableRollingMinMaxSpec of(boolean isMax) {
    return new ImmutableRollingMinMaxSpec(isMax);
  }

  /**
   * Creates an immutable copy of a {@link RollingMinMaxSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingMinMaxSpec instance
   */
  public static ImmutableRollingMinMaxSpec copyOf(RollingMinMaxSpec instance) {
    if (instance instanceof ImmutableRollingMinMaxSpec) {
      return (ImmutableRollingMinMaxSpec) instance;
    }
    return ImmutableRollingMinMaxSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
   * <pre>
   * ImmutableRollingMinMaxSpec.builder()
   *    .isMax(boolean) // required {@link RollingMinMaxSpec#isMax() isMax}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingMinMaxSpec builder
   */
  public static ImmutableRollingMinMaxSpec.Builder builder() {
    return new ImmutableRollingMinMaxSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingMinMaxSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_IS_MAX = 0x1L;
    private long initBits = 0x1L;

    private boolean isMax;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingMinMaxSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingMinMaxSpec instance) {
      Objects.requireNonNull(instance, "instance");
      isMax(instance.isMax());
      return this;
    }

    /**
     * Initializes the value for the {@link RollingMinMaxSpec#isMax() isMax} attribute.
     * @param isMax The value for isMax 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isMax(boolean isMax) {
      this.isMax = isMax;
      initBits &= ~INIT_BIT_IS_MAX;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingMinMaxSpec ImmutableRollingMinMaxSpec}.
     * @return An immutable instance of RollingMinMaxSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingMinMaxSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableRollingMinMaxSpec(isMax);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_IS_MAX) != 0) attributes.add("isMax");
      return "Cannot build RollingMinMaxSpec, some of required attributes are not set " + attributes;
    }
  }
}
