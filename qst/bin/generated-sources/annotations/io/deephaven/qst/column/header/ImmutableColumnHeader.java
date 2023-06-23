package io.deephaven.qst.column.header;

import io.deephaven.qst.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeader}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeader.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeader.of()}.
 */
@Generated(from = "ColumnHeader", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeader<T1> extends ColumnHeader<T1> {
  private final String name;
  private final Type<T1> componentType;

  private ImmutableColumnHeader(String name, Type<T1> componentType) {
    this.name = Objects.requireNonNull(name, "name");
    this.componentType = Objects.requireNonNull(componentType, "componentType");
  }

  private ImmutableColumnHeader(
      ImmutableColumnHeader<T1> original,
      String name,
      Type<T1> componentType) {
    this.name = name;
    this.componentType = componentType;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code componentType} attribute
   */
  @Override
  public Type<T1> componentType() {
    return componentType;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeader#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeader<T1> withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableColumnHeader<>(this, newValue, this.componentType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeader#componentType() componentType} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for componentType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeader<T1> withComponentType(Type<T1> value) {
    if (this.componentType == value) return this;
    Type<T1> newValue = Objects.requireNonNull(value, "componentType");
    return validate(new ImmutableColumnHeader<>(this, this.name, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeader} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeader<?>
        && equalTo(0, (ImmutableColumnHeader<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeader<?> another) {
    return name.equals(another.name)
        && componentType.equals(another.componentType);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code componentType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + componentType.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeader} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeader{"
        + "name=" + name
        + ", componentType=" + componentType
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeader} instance.
 * @param <T1> generic parameter T1
   * @param name The value for the {@code name} attribute
   * @param componentType The value for the {@code componentType} attribute
   * @return An immutable ColumnHeader instance
   */
  public static <T1> ImmutableColumnHeader<T1> of(String name, Type<T1> componentType) {
    return validate(new ImmutableColumnHeader<>(name, componentType));
  }

  private static <T1> ImmutableColumnHeader<T1> validate(ImmutableColumnHeader<T1> instance) {
    instance.checkName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeader} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeader instance
   */
  public static <T1> ImmutableColumnHeader<T1> copyOf(ColumnHeader<T1> instance) {
    if (instance instanceof ImmutableColumnHeader<?>) {
      return (ImmutableColumnHeader<T1>) instance;
    }
    return ImmutableColumnHeader.<T1>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeader ImmutableColumnHeader}.
   * <pre>
   * ImmutableColumnHeader.&amp;lt;T1&amp;gt;builder()
   *    .name(String) // required {@link ColumnHeader#name() name}
   *    .componentType(io.deephaven.qst.type.Type&amp;lt;T1&amp;gt;) // required {@link ColumnHeader#componentType() componentType}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @return A new ImmutableColumnHeader builder
   */
  public static <T1> ImmutableColumnHeader.Builder<T1> builder() {
    return new ImmutableColumnHeader.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeader ImmutableColumnHeader}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeader", generator = "Immutables")
  public static final class Builder<T1> {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_COMPONENT_TYPE = 0x2L;
    private long initBits = 0x3L;

    private String name;
    private Type<T1> componentType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeader} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1> from(ColumnHeader<T1> instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      componentType(instance.componentType());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeader#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1> name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeader#componentType() componentType} attribute.
     * @param componentType The value for componentType 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1> componentType(Type<T1> componentType) {
      this.componentType = Objects.requireNonNull(componentType, "componentType");
      initBits &= ~INIT_BIT_COMPONENT_TYPE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeader ImmutableColumnHeader}.
     * @return An immutable instance of ColumnHeader
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeader<T1> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumnHeader.validate(new ImmutableColumnHeader<>(null, name, componentType));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_COMPONENT_TYPE) != 0) attributes.add("componentType");
      return "Cannot build ColumnHeader, some of required attributes are not set " + attributes;
    }
  }
}
