package io.deephaven.api.literal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LiteralDouble}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralDouble.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralDouble.of()}.
 */
@Generated(from = "LiteralDouble", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralDouble extends LiteralDouble {
  private final double value;

  private ImmutableLiteralDouble(double value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public double value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralDouble#value() value} attribute.
   * A value strict bits equality used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralDouble withValue(double value) {
    if (Double.doubleToLongBits(this.value) == Double.doubleToLongBits(value)) return this;
    return validate(new ImmutableLiteralDouble(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralDouble} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralDouble
        && equalTo(0, (ImmutableLiteralDouble) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralDouble another) {
    return Double.doubleToLongBits(value) == Double.doubleToLongBits(another.value);
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Double.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralDouble} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralDouble{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralDouble} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralDouble instance
   */
  public static ImmutableLiteralDouble of(double value) {
    return validate(new ImmutableLiteralDouble(value));
  }

  private static ImmutableLiteralDouble validate(ImmutableLiteralDouble instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralDouble} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralDouble instance
   */
  public static ImmutableLiteralDouble copyOf(LiteralDouble instance) {
    if (instance instanceof ImmutableLiteralDouble) {
      return (ImmutableLiteralDouble) instance;
    }
    return ImmutableLiteralDouble.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralDouble ImmutableLiteralDouble}.
   * <pre>
   * ImmutableLiteralDouble.builder()
   *    .value(double) // required {@link LiteralDouble#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralDouble builder
   */
  public static ImmutableLiteralDouble.Builder builder() {
    return new ImmutableLiteralDouble.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralDouble ImmutableLiteralDouble}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralDouble", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private double value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralDouble} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralDouble instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralDouble#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(double value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralDouble ImmutableLiteralDouble}.
     * @return An immutable instance of LiteralDouble
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralDouble build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralDouble.validate(new ImmutableLiteralDouble(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralDouble, some of required attributes are not set " + attributes;
    }
  }
}
