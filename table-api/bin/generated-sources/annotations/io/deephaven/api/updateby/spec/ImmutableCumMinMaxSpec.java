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
 * Immutable implementation of {@link CumMinMaxSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCumMinMaxSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableCumMinMaxSpec.of()}.
 */
@Generated(from = "CumMinMaxSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableCumMinMaxSpec extends CumMinMaxSpec {
  private final boolean isMax;

  private ImmutableCumMinMaxSpec(boolean isMax) {
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
   * Copy the current immutable object by setting a value for the {@link CumMinMaxSpec#isMax() isMax} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isMax
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCumMinMaxSpec withIsMax(boolean value) {
    if (this.isMax == value) return this;
    return new ImmutableCumMinMaxSpec(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCumMinMaxSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCumMinMaxSpec
        && equalTo(0, (ImmutableCumMinMaxSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableCumMinMaxSpec another) {
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
   * Prints the immutable value {@code CumMinMaxSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CumMinMaxSpec{"
        + "isMax=" + isMax
        + "}";
  }

  /**
   * Construct a new immutable {@code CumMinMaxSpec} instance.
   * @param isMax The value for the {@code isMax} attribute
   * @return An immutable CumMinMaxSpec instance
   */
  public static ImmutableCumMinMaxSpec of(boolean isMax) {
    return new ImmutableCumMinMaxSpec(isMax);
  }

  /**
   * Creates an immutable copy of a {@link CumMinMaxSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CumMinMaxSpec instance
   */
  public static ImmutableCumMinMaxSpec copyOf(CumMinMaxSpec instance) {
    if (instance instanceof ImmutableCumMinMaxSpec) {
      return (ImmutableCumMinMaxSpec) instance;
    }
    return ImmutableCumMinMaxSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCumMinMaxSpec ImmutableCumMinMaxSpec}.
   * <pre>
   * ImmutableCumMinMaxSpec.builder()
   *    .isMax(boolean) // required {@link CumMinMaxSpec#isMax() isMax}
   *    .build();
   * </pre>
   * @return A new ImmutableCumMinMaxSpec builder
   */
  public static ImmutableCumMinMaxSpec.Builder builder() {
    return new ImmutableCumMinMaxSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCumMinMaxSpec ImmutableCumMinMaxSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CumMinMaxSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_IS_MAX = 0x1L;
    private long initBits = 0x1L;

    private boolean isMax;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CumMinMaxSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CumMinMaxSpec instance) {
      Objects.requireNonNull(instance, "instance");
      isMax(instance.isMax());
      return this;
    }

    /**
     * Initializes the value for the {@link CumMinMaxSpec#isMax() isMax} attribute.
     * @param isMax The value for isMax 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isMax(boolean isMax) {
      this.isMax = isMax;
      initBits &= ~INIT_BIT_IS_MAX;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCumMinMaxSpec ImmutableCumMinMaxSpec}.
     * @return An immutable instance of CumMinMaxSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCumMinMaxSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableCumMinMaxSpec(isMax);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_IS_MAX) != 0) attributes.add("isMax");
      return "Cannot build CumMinMaxSpec, some of required attributes are not set " + attributes;
    }
  }
}
