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
 * Immutable implementation of {@link LiteralShort}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralShort.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralShort.of()}.
 */
@Generated(from = "LiteralShort", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralShort extends LiteralShort {
  private final short value;

  private ImmutableLiteralShort(short value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public short value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralShort#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralShort withValue(short value) {
    if (this.value == value) return this;
    return validate(new ImmutableLiteralShort(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralShort} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralShort
        && equalTo(0, (ImmutableLiteralShort) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralShort another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Short.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralShort} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralShort{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralShort} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralShort instance
   */
  public static ImmutableLiteralShort of(short value) {
    return validate(new ImmutableLiteralShort(value));
  }

  private static ImmutableLiteralShort validate(ImmutableLiteralShort instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralShort} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralShort instance
   */
  public static ImmutableLiteralShort copyOf(LiteralShort instance) {
    if (instance instanceof ImmutableLiteralShort) {
      return (ImmutableLiteralShort) instance;
    }
    return ImmutableLiteralShort.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralShort ImmutableLiteralShort}.
   * <pre>
   * ImmutableLiteralShort.builder()
   *    .value(short) // required {@link LiteralShort#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralShort builder
   */
  public static ImmutableLiteralShort.Builder builder() {
    return new ImmutableLiteralShort.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralShort ImmutableLiteralShort}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralShort", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private short value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralShort} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralShort instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralShort#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(short value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralShort ImmutableLiteralShort}.
     * @return An immutable instance of LiteralShort
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralShort build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralShort.validate(new ImmutableLiteralShort(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralShort, some of required attributes are not set " + attributes;
    }
  }
}
