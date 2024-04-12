package io.deephaven.qst.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link GenericVectorType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableGenericVectorType.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableGenericVectorType.of()}.
 */
@Generated(from = "GenericVectorType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableGenericVectorType<T, ComponentType>
    extends GenericVectorType<T, ComponentType> {
  private final Class<T> clazz;
  private final GenericType<ComponentType> componentType;

  private ImmutableGenericVectorType(Class<T> clazz, GenericType<ComponentType> componentType) {
    this.clazz = Objects.requireNonNull(clazz, "clazz");
    this.componentType = Objects.requireNonNull(componentType, "componentType");
  }

  private ImmutableGenericVectorType(
      ImmutableGenericVectorType<T, ComponentType> original,
      Class<T> clazz,
      GenericType<ComponentType> componentType) {
    this.clazz = clazz;
    this.componentType = componentType;
  }

  /**
   * @return The value of the {@code clazz} attribute
   */
  @Override
  public Class<T> clazz() {
    return clazz;
  }

  /**
   * @return The value of the {@code componentType} attribute
   */
  @Override
  public GenericType<ComponentType> componentType() {
    return componentType;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link GenericVectorType#clazz() clazz} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clazz
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableGenericVectorType<T, ComponentType> withClazz(Class<T> value) {
    if (this.clazz == value) return this;
    Class<T> newValue = Objects.requireNonNull(value, "clazz");
    return new ImmutableGenericVectorType<>(this, newValue, this.componentType);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link GenericVectorType#componentType() componentType} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for componentType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableGenericVectorType<T, ComponentType> withComponentType(GenericType<ComponentType> value) {
    if (this.componentType == value) return this;
    GenericType<ComponentType> newValue = Objects.requireNonNull(value, "componentType");
    return new ImmutableGenericVectorType<>(this, this.clazz, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableGenericVectorType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableGenericVectorType<?, ?>
        && equalTo(0, (ImmutableGenericVectorType<?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableGenericVectorType<?, ?> another) {
    return clazz.equals(another.clazz)
        && componentType.equals(another.componentType);
  }

  /**
   * Computes a hash code from attributes: {@code clazz}, {@code componentType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + clazz.hashCode();
    h += (h << 5) + componentType.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code GenericVectorType} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "GenericVectorType{"
        + "clazz=" + clazz
        + ", componentType=" + componentType
        + "}";
  }

  /**
   * Construct a new immutable {@code GenericVectorType} instance.
 * @param <T> generic parameter T
 * @param <ComponentType> generic parameter ComponentType
   * @param clazz The value for the {@code clazz} attribute
   * @param componentType The value for the {@code componentType} attribute
   * @return An immutable GenericVectorType instance
   */
  public static <T, ComponentType> ImmutableGenericVectorType<T, ComponentType> of(Class<T> clazz, GenericType<ComponentType> componentType) {
    return new ImmutableGenericVectorType<>(clazz, componentType);
  }

  /**
   * Creates an immutable copy of a {@link GenericVectorType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param <ComponentType> generic parameter ComponentType
   * @param instance The instance to copy
   * @return A copied immutable GenericVectorType instance
   */
  public static <T, ComponentType> ImmutableGenericVectorType<T, ComponentType> copyOf(GenericVectorType<T, ComponentType> instance) {
    if (instance instanceof ImmutableGenericVectorType<?, ?>) {
      return (ImmutableGenericVectorType<T, ComponentType>) instance;
    }
    return ImmutableGenericVectorType.<T, ComponentType>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableGenericVectorType ImmutableGenericVectorType}.
   * <pre>
   * ImmutableGenericVectorType.&amp;lt;T, ComponentType&amp;gt;builder()
   *    .clazz(Class&amp;lt;T&amp;gt;) // required {@link GenericVectorType#clazz() clazz}
   *    .componentType(io.deephaven.qst.type.GenericType&amp;lt;ComponentType&amp;gt;) // required {@link GenericVectorType#componentType() componentType}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @param <ComponentType> generic parameter ComponentType
   * @return A new ImmutableGenericVectorType builder
   */
  public static <T, ComponentType> ImmutableGenericVectorType.Builder<T, ComponentType> builder() {
    return new ImmutableGenericVectorType.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableGenericVectorType ImmutableGenericVectorType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "GenericVectorType", generator = "Immutables")
  public static final class Builder<T, ComponentType> {
    private static final long INIT_BIT_CLAZZ = 0x1L;
    private static final long INIT_BIT_COMPONENT_TYPE = 0x2L;
    private long initBits = 0x3L;

    private Class<T> clazz;
    private GenericType<ComponentType> componentType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code GenericVectorType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> from(GenericVectorType<T, ComponentType> instance) {
      Objects.requireNonNull(instance, "instance");
      clazz(instance.clazz());
      componentType(instance.componentType());
      return this;
    }

    /**
     * Initializes the value for the {@link GenericVectorType#clazz() clazz} attribute.
     * @param clazz The value for clazz 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> clazz(Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      initBits &= ~INIT_BIT_CLAZZ;
      return this;
    }

    /**
     * Initializes the value for the {@link GenericVectorType#componentType() componentType} attribute.
     * @param componentType The value for componentType 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> componentType(GenericType<ComponentType> componentType) {
      this.componentType = Objects.requireNonNull(componentType, "componentType");
      initBits &= ~INIT_BIT_COMPONENT_TYPE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableGenericVectorType ImmutableGenericVectorType}.
     * @return An immutable instance of GenericVectorType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableGenericVectorType<T, ComponentType> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableGenericVectorType<>(null, clazz, componentType);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLAZZ) != 0) attributes.add("clazz");
      if ((initBits & INIT_BIT_COMPONENT_TYPE) != 0) attributes.add("componentType");
      return "Cannot build GenericVectorType, some of required attributes are not set " + attributes;
    }
  }
}
