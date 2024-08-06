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
 * Immutable implementation of {@link LiteralByte}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralByte.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralByte.of()}.
 */
@Generated(from = "LiteralByte", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralByte extends LiteralByte {
  private final byte value;

  private ImmutableLiteralByte(byte value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public byte value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralByte#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralByte withValue(byte value) {
    if (this.value == value) return this;
    return validate(new ImmutableLiteralByte(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralByte} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralByte
        && equalTo(0, (ImmutableLiteralByte) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralByte another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Byte.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralByte} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralByte{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralByte} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralByte instance
   */
  public static ImmutableLiteralByte of(byte value) {
    return validate(new ImmutableLiteralByte(value));
  }

  private static ImmutableLiteralByte validate(ImmutableLiteralByte instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralByte} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralByte instance
   */
  public static ImmutableLiteralByte copyOf(LiteralByte instance) {
    if (instance instanceof ImmutableLiteralByte) {
      return (ImmutableLiteralByte) instance;
    }
    return ImmutableLiteralByte.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralByte ImmutableLiteralByte}.
   * <pre>
   * ImmutableLiteralByte.builder()
   *    .value(byte) // required {@link LiteralByte#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralByte builder
   */
  public static ImmutableLiteralByte.Builder builder() {
    return new ImmutableLiteralByte.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralByte ImmutableLiteralByte}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralByte", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private byte value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralByte} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralByte instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralByte#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(byte value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralByte ImmutableLiteralByte}.
     * @return An immutable instance of LiteralByte
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralByte build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralByte.validate(new ImmutableLiteralByte(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralByte, some of required attributes are not set " + attributes;
    }
  }
}
