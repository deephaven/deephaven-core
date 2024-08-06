package io.deephaven.qst;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LabeledValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLabeledValue.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLabeledValue.of()}.
 */
@Generated(from = "LabeledValue", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableLabeledValue<T> extends LabeledValue<T> {
  private final String name;
  private final T value;

  private ImmutableLabeledValue(String name, T value) {
    this.name = Objects.requireNonNull(name, "name");
    this.value = Objects.requireNonNull(value, "value");
  }

  private ImmutableLabeledValue(ImmutableLabeledValue<T> original, String name, T value) {
    this.name = name;
    this.value = value;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public T value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LabeledValue#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLabeledValue<T> withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new ImmutableLabeledValue<>(this, newValue, this.value);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LabeledValue#value() value} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLabeledValue<T> withValue(T value) {
    if (this.value == value) return this;
    T newValue = Objects.requireNonNull(value, "value");
    return new ImmutableLabeledValue<>(this, this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLabeledValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLabeledValue<?>
        && equalTo(0, (ImmutableLabeledValue<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableLabeledValue<?> another) {
    return name.equals(another.name)
        && value.equals(another.value);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + value.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code LabeledValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LabeledValue{"
        + "name=" + name
        + ", value=" + value
        + "}";
  }

  /**
   * Construct a new immutable {@code LabeledValue} instance.
 * @param <T> generic parameter T
   * @param name The value for the {@code name} attribute
   * @param value The value for the {@code value} attribute
   * @return An immutable LabeledValue instance
   */
  public static <T> ImmutableLabeledValue<T> of(String name, T value) {
    return new ImmutableLabeledValue<>(name, value);
  }

  /**
   * Creates an immutable copy of a {@link LabeledValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable LabeledValue instance
   */
  public static <T> ImmutableLabeledValue<T> copyOf(LabeledValue<T> instance) {
    if (instance instanceof ImmutableLabeledValue<?>) {
      return (ImmutableLabeledValue<T>) instance;
    }
    return ImmutableLabeledValue.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLabeledValue ImmutableLabeledValue}.
   * <pre>
   * ImmutableLabeledValue.&amp;lt;T&amp;gt;builder()
   *    .name(String) // required {@link LabeledValue#name() name}
   *    .value(T) // required {@link LabeledValue#value() value}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableLabeledValue builder
   */
  public static <T> ImmutableLabeledValue.Builder<T> builder() {
    return new ImmutableLabeledValue.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableLabeledValue ImmutableLabeledValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LabeledValue", generator = "Immutables")
  public static final class Builder<T> {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_VALUE = 0x2L;
    private long initBits = 0x3L;

    private String name;
    private T value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LabeledValue} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(LabeledValue<T> instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link LabeledValue#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link LabeledValue#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> value(T value) {
      this.value = Objects.requireNonNull(value, "value");
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLabeledValue ImmutableLabeledValue}.
     * @return An immutable instance of LabeledValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLabeledValue<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableLabeledValue<>(null, name, value);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build LabeledValue, some of required attributes are not set " + attributes;
    }
  }
}
