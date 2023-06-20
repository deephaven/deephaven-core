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
 * Immutable implementation of {@link LiteralFloat}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralFloat.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralFloat.of()}.
 */
@Generated(from = "LiteralFloat", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralFloat extends LiteralFloat {
  private final float value;

  private ImmutableLiteralFloat(float value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public float value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralFloat#value() value} attribute.
   * A value strict bits equality used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralFloat withValue(float value) {
    if (Float.floatToIntBits(this.value) == Float.floatToIntBits(value)) return this;
    return validate(new ImmutableLiteralFloat(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralFloat} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralFloat
        && equalTo(0, (ImmutableLiteralFloat) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralFloat another) {
    return Float.floatToIntBits(value) == Float.floatToIntBits(another.value);
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Float.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralFloat} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralFloat{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralFloat} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralFloat instance
   */
  public static ImmutableLiteralFloat of(float value) {
    return validate(new ImmutableLiteralFloat(value));
  }

  private static ImmutableLiteralFloat validate(ImmutableLiteralFloat instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralFloat} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralFloat instance
   */
  public static ImmutableLiteralFloat copyOf(LiteralFloat instance) {
    if (instance instanceof ImmutableLiteralFloat) {
      return (ImmutableLiteralFloat) instance;
    }
    return ImmutableLiteralFloat.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralFloat ImmutableLiteralFloat}.
   * <pre>
   * ImmutableLiteralFloat.builder()
   *    .value(float) // required {@link LiteralFloat#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralFloat builder
   */
  public static ImmutableLiteralFloat.Builder builder() {
    return new ImmutableLiteralFloat.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralFloat ImmutableLiteralFloat}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralFloat", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private float value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralFloat} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralFloat instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralFloat#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(float value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralFloat ImmutableLiteralFloat}.
     * @return An immutable instance of LiteralFloat
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralFloat build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralFloat.validate(new ImmutableLiteralFloat(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralFloat, some of required attributes are not set " + attributes;
    }
  }
}
