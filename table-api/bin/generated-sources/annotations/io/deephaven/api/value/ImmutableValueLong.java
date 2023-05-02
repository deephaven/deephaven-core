package io.deephaven.api.value;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ValueLong}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableValueLong.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableValueLong.of()}.
 */
@Generated(from = "ValueLong", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableValueLong extends ValueLong {
  private final long value;

  private ImmutableValueLong(long value) {
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
   * Copy the current immutable object by setting a value for the {@link ValueLong#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableValueLong withValue(long value) {
    if (this.value == value) return this;
    return validate(new ImmutableValueLong(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableValueLong} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableValueLong
        && equalTo(0, (ImmutableValueLong) another);
  }

  private boolean equalTo(int synthetic, ImmutableValueLong another) {
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
   * Prints the immutable value {@code ValueLong} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ValueLong{"
        + "value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code ValueLong} instance.
   * @param value The value for the {@code value} attribute
   * @return An immutable ValueLong instance
   */
  public static ImmutableValueLong of(long value) {
    return validate(new ImmutableValueLong(value));
  }

  private static ImmutableValueLong validate(ImmutableValueLong instance) {
    instance.checkNotDeephavenNull();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ValueLong} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ValueLong instance
   */
  public static ImmutableValueLong copyOf(ValueLong instance) {
    if (instance instanceof ImmutableValueLong) {
      return (ImmutableValueLong) instance;
    }
    return ImmutableValueLong.builder()
        .from(instance)
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableValueLong ImmutableValueLong}.
   * <pre>
   * ImmutableValueLong.builder()
   *    .value(long) // required {@link ValueLong#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableValueLong builder
   */
  public static ImmutableValueLong.Builder builder() {
    return new ImmutableValueLong.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableValueLong ImmutableValueLong}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ValueLong", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private long initBits = 0x1L;

    private long value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ValueLong} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ValueLong instance) {
      Objects.requireNonNull(instance, "instance");
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link ValueLong#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(long value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableValueLong ImmutableValueLong}.
     * @return An immutable instance of ValueLong
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableValueLong build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableValueLong.validate(new ImmutableValueLong(value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build ValueLong, some of required attributes are not set " + attributes;
    }
  }
}
