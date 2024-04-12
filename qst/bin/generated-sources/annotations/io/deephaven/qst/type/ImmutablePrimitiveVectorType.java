package io.deephaven.qst.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link PrimitiveVectorType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePrimitiveVectorType.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutablePrimitiveVectorType.of()}.
 */
@Generated(from = "PrimitiveVectorType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutablePrimitiveVectorType<T, ComponentType>
    extends PrimitiveVectorType<T, ComponentType> {
  private final Class<T> clazz;
  private final PrimitiveType<ComponentType> componentType;

  private ImmutablePrimitiveVectorType(Class<T> clazz, PrimitiveType<ComponentType> componentType) {
    this.clazz = Objects.requireNonNull(clazz, "clazz");
    this.componentType = Objects.requireNonNull(componentType, "componentType");
  }

  private ImmutablePrimitiveVectorType(
      ImmutablePrimitiveVectorType<T, ComponentType> original,
      Class<T> clazz,
      PrimitiveType<ComponentType> componentType) {
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
  public PrimitiveType<ComponentType> componentType() {
    return componentType;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PrimitiveVectorType#clazz() clazz} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clazz
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePrimitiveVectorType<T, ComponentType> withClazz(Class<T> value) {
    if (this.clazz == value) return this;
    Class<T> newValue = Objects.requireNonNull(value, "clazz");
    return validate(new ImmutablePrimitiveVectorType<>(this, newValue, this.componentType));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PrimitiveVectorType#componentType() componentType} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for componentType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePrimitiveVectorType<T, ComponentType> withComponentType(PrimitiveType<ComponentType> value) {
    if (this.componentType == value) return this;
    PrimitiveType<ComponentType> newValue = Objects.requireNonNull(value, "componentType");
    return validate(new ImmutablePrimitiveVectorType<>(this, this.clazz, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePrimitiveVectorType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePrimitiveVectorType<?, ?>
        && equalTo(0, (ImmutablePrimitiveVectorType<?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutablePrimitiveVectorType<?, ?> another) {
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
   * Prints the immutable value {@code PrimitiveVectorType} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "PrimitiveVectorType{"
        + "clazz=" + clazz
        + ", componentType=" + componentType
        + "}";
  }

  /**
   * Construct a new immutable {@code PrimitiveVectorType} instance.
 * @param <T> generic parameter T
 * @param <ComponentType> generic parameter ComponentType
   * @param clazz The value for the {@code clazz} attribute
   * @param componentType The value for the {@code componentType} attribute
   * @return An immutable PrimitiveVectorType instance
   */
  public static <T, ComponentType> ImmutablePrimitiveVectorType<T, ComponentType> of(Class<T> clazz, PrimitiveType<ComponentType> componentType) {
    return validate(new ImmutablePrimitiveVectorType<>(clazz, componentType));
  }

  private static <T, ComponentType> ImmutablePrimitiveVectorType<T, ComponentType> validate(ImmutablePrimitiveVectorType<T, ComponentType> instance) {
    instance.checkClazz();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link PrimitiveVectorType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param <ComponentType> generic parameter ComponentType
   * @param instance The instance to copy
   * @return A copied immutable PrimitiveVectorType instance
   */
  public static <T, ComponentType> ImmutablePrimitiveVectorType<T, ComponentType> copyOf(PrimitiveVectorType<T, ComponentType> instance) {
    if (instance instanceof ImmutablePrimitiveVectorType<?, ?>) {
      return (ImmutablePrimitiveVectorType<T, ComponentType>) instance;
    }
    return ImmutablePrimitiveVectorType.<T, ComponentType>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutablePrimitiveVectorType ImmutablePrimitiveVectorType}.
   * <pre>
   * ImmutablePrimitiveVectorType.&amp;lt;T, ComponentType&amp;gt;builder()
   *    .clazz(Class&amp;lt;T&amp;gt;) // required {@link PrimitiveVectorType#clazz() clazz}
   *    .componentType(io.deephaven.qst.type.PrimitiveType&amp;lt;ComponentType&amp;gt;) // required {@link PrimitiveVectorType#componentType() componentType}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @param <ComponentType> generic parameter ComponentType
   * @return A new ImmutablePrimitiveVectorType builder
   */
  public static <T, ComponentType> ImmutablePrimitiveVectorType.Builder<T, ComponentType> builder() {
    return new ImmutablePrimitiveVectorType.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutablePrimitiveVectorType ImmutablePrimitiveVectorType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "PrimitiveVectorType", generator = "Immutables")
  public static final class Builder<T, ComponentType> {
    private static final long INIT_BIT_CLAZZ = 0x1L;
    private static final long INIT_BIT_COMPONENT_TYPE = 0x2L;
    private long initBits = 0x3L;

    private Class<T> clazz;
    private PrimitiveType<ComponentType> componentType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code PrimitiveVectorType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> from(PrimitiveVectorType<T, ComponentType> instance) {
      Objects.requireNonNull(instance, "instance");
      clazz(instance.clazz());
      componentType(instance.componentType());
      return this;
    }

    /**
     * Initializes the value for the {@link PrimitiveVectorType#clazz() clazz} attribute.
     * @param clazz The value for clazz 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> clazz(Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      initBits &= ~INIT_BIT_CLAZZ;
      return this;
    }

    /**
     * Initializes the value for the {@link PrimitiveVectorType#componentType() componentType} attribute.
     * @param componentType The value for componentType 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T, ComponentType> componentType(PrimitiveType<ComponentType> componentType) {
      this.componentType = Objects.requireNonNull(componentType, "componentType");
      initBits &= ~INIT_BIT_COMPONENT_TYPE;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePrimitiveVectorType ImmutablePrimitiveVectorType}.
     * @return An immutable instance of PrimitiveVectorType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePrimitiveVectorType<T, ComponentType> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutablePrimitiveVectorType.validate(new ImmutablePrimitiveVectorType<>(null, clazz, componentType));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLAZZ) != 0) attributes.add("clazz");
      if ((initBits & INIT_BIT_COMPONENT_TYPE) != 0) attributes.add("componentType");
      return "Cannot build PrimitiveVectorType, some of required attributes are not set " + attributes;
    }
  }
}
