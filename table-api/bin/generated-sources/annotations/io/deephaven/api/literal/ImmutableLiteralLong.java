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
 * Immutable implementation of {@link LiteralLong}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLiteralLong.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLiteralLong.of()}.
 */
@Generated(from = "LiteralLong", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableLiteralLong extends LiteralLong {
  private final long value;

  private ImmutableLiteralLong(long value) {
    this.value = value;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public long value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LiteralLong#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLiteralLong withValue(long value) {
    if (this.value == value) return this;
    return validate(new ImmutableLiteralLong(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLiteralLong} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLiteralLong
        && equalTo(0, (ImmutableLiteralLong) another);
  }

  private boolean equalTo(int synthetic, ImmutableLiteralLong another) {
    return value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Long.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code LiteralLong} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LiteralLong{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LiteralLong} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable LiteralLong instance
   */
  public static ImmutableLiteralLong of(long value) {
    return validate(new ImmutableLiteralLong(value));
  }

  private static ImmutableLiteralLong validate(ImmutableLiteralLong instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LiteralLong} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LiteralLong instance
   */
  public static ImmutableLiteralLong copyOf(LiteralLong instance) {
    if (instance instanceof ImmutableLiteralLong) {
      return (ImmutableLiteralLong) instance;
    }
    return ImmutableLiteralLong.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLiteralLong ImmutableLiteralLong}.
   * <pre>
   * ImmutableLiteralLong.builder()
   *    .value(long) // required {@link LiteralLong#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableLiteralLong builder
   */
  public static ImmutableLiteralLong.Builder builder() {
    return new ImmutableLiteralLong.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLiteralLong ImmutableLiteralLong}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LiteralLong", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private long value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LiteralLong} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LiteralLong instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LiteralLong#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(long value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLiteralLong ImmutableLiteralLong}.
     * @return An immutable instance of LiteralLong
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLiteralLong build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLiteralLong.validate(new ImmutableLiteralLong(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LiteralLong, some of required attributes are not set " + attributes;
    }
  }
}
