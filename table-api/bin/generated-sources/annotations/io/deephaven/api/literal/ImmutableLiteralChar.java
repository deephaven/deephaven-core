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
 * Immutable implementation of {@link LiteralChar}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralChar.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralChar.of()}.
 */
@Generated(from = "LiteralChar", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralChar extends LiteralChar {
  private final char value;

  private ImmutableLiteralChar(char value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public char value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralChar#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralChar withValue(char value) {
    if (this.value == value) return this;
    return validate(new ImmutableLiteralChar(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralChar} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralChar
        && equalTo(0, (ImmutableLiteralChar) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralChar another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Character.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralChar} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralChar{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralChar} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralChar instance
   */
  public static ImmutableLiteralChar of(char value) {
    return validate(new ImmutableLiteralChar(value));
  }

  private static ImmutableLiteralChar validate(ImmutableLiteralChar instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralChar} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralChar instance
   */
  public static ImmutableLiteralChar copyOf(LiteralChar instance) {
    if (instance instanceof ImmutableLiteralChar) {
      return (ImmutableLiteralChar) instance;
    }
    return ImmutableLiteralChar.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralChar ImmutableLiteralChar}.
   * <pre>
   * ImmutableLiteralChar.builder()
   *    .value(char) // required {@link LiteralChar#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralChar builder
   */
  public static ImmutableLiteralChar.Builder builder() {
    return new ImmutableLiteralChar.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralChar ImmutableLiteralChar}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralChar", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private char value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralChar} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralChar instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralChar#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(char value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralChar ImmutableLiteralChar}.
     * @return An immutable instance of LiteralChar
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralChar build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralChar.validate(new ImmutableLiteralChar(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralChar, some of required attributes are not set " + attributes;
    }
  }
}
