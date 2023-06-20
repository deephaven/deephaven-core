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
 * Immutable implementation of {@link LiteralString}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralString.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralString.of()}.
 */
@Generated(from = "LiteralString", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralString extends LiteralString {
  private final String value;

  private ImmutableLiteralString(String value) {
    this.value = Objects.requireNonNull(value, "value");
  }

  private ImmutableLiteralString(ImmutableLiteralString original, String value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public String value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralString#value() value} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralString withValue(String value) {
    String newValue = Objects.requireNonNull(value, "value");
    if (this.value.equals(newValue)) return this;
    return new ImmutableLiteralString(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralString} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralString
        && equalTo(0, (ImmutableLiteralString) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralString another) {
    return value.equals(another.value);
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + value.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralString} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralString{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralString} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralString instance
   */
  public static ImmutableLiteralString of(String value) {
    return new ImmutableLiteralString(value);
  }

  /**
   * Creates an immutable copy of a {@link LiteralString} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralString instance
   */
  public static ImmutableLiteralString copyOf(LiteralString instance) {
    if (instance instanceof ImmutableLiteralString) {
      return (ImmutableLiteralString) instance;
    }
    return ImmutableLiteralString.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralString ImmutableLiteralString}.
   * <pre>
   * ImmutableLiteralString.builder()
   *    .value(String) // required {@link LiteralString#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralString builder
   */
  public static ImmutableLiteralString.Builder builder() {
    return new ImmutableLiteralString.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralString ImmutableLiteralString}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralString", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralString} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralString instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralString#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(String value) {
      this.value = Objects.requireNonNull(value, "value");
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralString ImmutableLiteralString}.
     * @return An immutable instance of LiteralString
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralString build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableLiteralString(null, value);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralString, some of required attributes are not set " + attributes;
    }
  }
}
