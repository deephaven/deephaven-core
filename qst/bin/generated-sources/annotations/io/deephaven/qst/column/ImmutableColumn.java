package io.deephaven.qst.column;

import io.deephaven.qst.array.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Column}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumn.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumn.of()}.
 */
@Generated(from = "Column", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumn<T> extends Column<T> {
  private final String name;
  private final Array<T> array;

  private ImmutableColumn(String name, Array<T> array) {
    this.name = Objects.requireNonNull(name, "name");
    this.array = Objects.requireNonNull(array, "array");
  }

  private ImmutableColumn(ImmutableColumn<T> original, String name, Array<T> array) {
    this.name = name;
    this.array = array;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code array} attribute
   */
  @Override
  public Array<T> array() {
    return array;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Column#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumn<T> withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableColumn<>(this, newValue, this.array));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Column#array() array} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for array
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumn<T> withArray(Array<T> value) {
    if (this.array == value) return this;
    Array<T> newValue = Objects.requireNonNull(value, "array");
    return validate(new ImmutableColumn<>(this, this.name, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumn} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumn<?>
        && equalTo(0, (ImmutableColumn<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumn<?> another) {
    return name.equals(another.name)
        && array.equals(another.array);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code array}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + array.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Column} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Column{"
        + "name=" + name
        + ", array=" + array
        + "}";
  }

  /**
   * Construct a new immutable {@code Column} instance.
 * @param <T> generic parameter T
   * @param name The value for the {@code name} attribute
   * @param array The value for the {@code array} attribute
   * @return An immutable Column instance
   */
  public static <T> ImmutableColumn<T> of(String name, Array<T> array) {
    return validate(new ImmutableColumn<>(name, array));
  }

  private static <T> ImmutableColumn<T> validate(ImmutableColumn<T> instance) {
    instance.checkName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link Column} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable Column instance
   */
  public static <T> ImmutableColumn<T> copyOf(Column<T> instance) {
    if (instance instanceof ImmutableColumn<?>) {
      return (ImmutableColumn<T>) instance;
    }
    return ImmutableColumn.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumn ImmutableColumn}.
   * <pre>
   * ImmutableColumn.&amp;lt;T&amp;gt;builder()
   *    .name(String) // required {@link Column#name() name}
   *    .array(io.deephaven.qst.array.Array&amp;lt;T&amp;gt;) // required {@link Column#array() array}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableColumn builder
   */
  public static <T> ImmutableColumn.Builder<T> builder() {
    return new ImmutableColumn.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumn ImmutableColumn}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Column", generator = "Immutables")
  public static final class Builder<T> {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_ARRAY = 0x2L;
    private long initBits = 0x3L;

    private String name;
    private Array<T> array;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Column} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(Column<T> instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      array(instance.array());
      return this;
    }

    /**
     * Initializes the value for the {@link Column#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link Column#array() array} attribute.
     * @param array The value for array 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> array(Array<T> array) {
      this.array = Objects.requireNonNull(array, "array");
      initBits &= ~INIT_BIT_ARRAY;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumn ImmutableColumn}.
     * @return An immutable instance of Column
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumn<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumn.validate(new ImmutableColumn<>(null, name, array));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_ARRAY) != 0) attributes.add("array");
      return "Cannot build Column, some of required attributes are not set " + attributes;
    }
  }
}
