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
 * Immutable implementation of {@link LiteralBool}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralBool.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralBool.of()}.
 */
@Generated(from = "LiteralBool", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralBool extends LiteralBool {
  private final boolean value;

  private ImmutableLiteralBool(boolean value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public boolean value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralBool#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralBool withValue(boolean value) {
    if (this.value == value) return this;
    return new ImmutableLiteralBool(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralBool} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralBool
        && equalTo(0, (ImmutableLiteralBool) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralBool another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralBool} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralBool{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralBool} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralBool instance
   */
  public static ImmutableLiteralBool of(boolean value) {
    return new ImmutableLiteralBool(value);
  }

  /**
   * Creates an immutable copy of a {@link LiteralBool} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralBool instance
   */
  public static ImmutableLiteralBool copyOf(LiteralBool instance) {
    if (instance instanceof ImmutableLiteralBool) {
      return (ImmutableLiteralBool) instance;
    }
    return ImmutableLiteralBool.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralBool ImmutableLiteralBool}.
   * <pre>
   * ImmutableLiteralBool.builder()
   *    .value(boolean) // required {@link LiteralBool#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralBool builder
   */
  public static ImmutableLiteralBool.Builder builder() {
    return new ImmutableLiteralBool.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralBool ImmutableLiteralBool}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralBool", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private boolean value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralBool} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralBool instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralBool#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(boolean value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralBool ImmutableLiteralBool}.
     * @return An immutable instance of LiteralBool
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralBool build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableLiteralBool(value);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralBool, some of required attributes are not set " + attributes;
    }
  }
}
