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
 * Immutable implementation of {@link LiteralInt}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralInt.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralInt.of()}.
 */
@Generated(from = "LiteralInt", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralInt extends LiteralInt {
  private final int value;

  private ImmutableLiteralInt(int value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public int value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralInt#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralInt withValue(int value) {
    if (this.value == value) return this;
    return validate(new ImmutableLiteralInt(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralInt} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralInt
        && equalTo(0, (ImmutableLiteralInt) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralInt another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + value;
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralInt} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralInt{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralInt} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralInt instance
   */
  public static ImmutableLiteralInt of(int value) {
    return validate(new ImmutableLiteralInt(value));
  }

  private static ImmutableLiteralInt validate(ImmutableLiteralInt instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralInt} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralInt instance
   */
  public static ImmutableLiteralInt copyOf(LiteralInt instance) {
    if (instance instanceof ImmutableLiteralInt) {
      return (ImmutableLiteralInt) instance;
    }
    return ImmutableLiteralInt.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralInt ImmutableLiteralInt}.
   * <pre>
   * ImmutableLiteralInt.builder()
   *    .value(int) // required {@link LiteralInt#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralInt builder
   */
  public static ImmutableLiteralInt.Builder builder() {
    return new ImmutableLiteralInt.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralInt ImmutableLiteralInt}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralInt", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private int value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralInt} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralInt instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralInt#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(int value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralInt ImmutableLiteralInt}.
     * @return An immutable instance of LiteralInt
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralInt build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralInt.validate(new ImmutableLiteralInt(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralInt, some of required attributes are not set " + attributes;
    }
  }
}
