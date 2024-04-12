package io.deephaven.qst.array;

import io.deephaven.qst.type.GenericType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link GenericArray}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableGenericArray.builder()}.
 */
@Generated(from = "GenericArray", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableGenericArray<T> extends GenericArray<T> {
  private final GenericType<T> componentType;
  private final List<T> values;

  private ImmutableGenericArray(GenericType<T> componentType, List<T> values) {
    this.componentType = componentType;
    this.values = values;
  }

  /**
   * @return The value of the {@code componentType} attribute
   */
  @Override
  public GenericType<T> componentType() {
    return componentType;
  }

  /**
   * @return The value of the {@code values} attribute
   */
  @Override
  public List<T> values() {
    return values;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link GenericArray#componentType() componentType} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for componentType
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableGenericArray<T> withComponentType(GenericType<T> value) {
    if (this.componentType == value) return this;
    GenericType<T> newValue = Objects.requireNonNull(value, "componentType");
    return new ImmutableGenericArray<>(newValue, this.values);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link GenericArray#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  @SafeVarargs @SuppressWarnings("varargs")
  public final ImmutableGenericArray<T> withValues(T... elements) {
    List<T> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), false, false));
    return new ImmutableGenericArray<>(this.componentType, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link GenericArray#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableGenericArray<T> withValues(Iterable<? extends T> elements) {
    if (this.values == elements) return this;
    List<T> newValue = createUnmodifiableList(false, createSafeList(elements, false, false));
    return new ImmutableGenericArray<>(this.componentType, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableGenericArray} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableGenericArray<?>
        && equalTo(0, (ImmutableGenericArray<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableGenericArray<?> another) {
    return componentType.equals(another.componentType)
        && values.equals(another.values);
  }

  /**
   * Computes a hash code from attributes: {@code componentType}, {@code values}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + componentType.hashCode();
    h += (h << 5) + values.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code GenericArray} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "GenericArray{"
        + "componentType=" + componentType
        + ", values=" + values
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link GenericArray} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable GenericArray instance
   */
  public static <T> ImmutableGenericArray<T> copyOf(GenericArray<T> instance) {
    if (instance instanceof ImmutableGenericArray<?>) {
      return (ImmutableGenericArray<T>) instance;
    }
    return ImmutableGenericArray.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableGenericArray ImmutableGenericArray}.
   * <pre>
   * ImmutableGenericArray.&amp;lt;T&amp;gt;builder()
   *    .componentType(io.deephaven.qst.type.GenericType&amp;lt;T&amp;gt;) // required {@link GenericArray#componentType() componentType}
   *    .addValues|addAllValues(T) // {@link GenericArray#values() values} elements
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableGenericArray builder
   */
  public static <T> ImmutableGenericArray.Builder<T> builder() {
    return new ImmutableGenericArray.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableGenericArray ImmutableGenericArray}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "GenericArray", generator = "Immutables")
  public static final class Builder<T> extends GenericArray.Builder<T> {
    private static final long INIT_BIT_COMPONENT_TYPE = 0x1L;
    private long initBits = 0x1L;

    private GenericType<T> componentType;
    private List<T> values = new ArrayList<T>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code GenericArray} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(GenericArray<T> instance) {
      Objects.requireNonNull(instance, "instance");
      componentType(instance.componentType());
      addAllValues(instance.values());
      return this;
    }

    /**
     * Initializes the value for the {@link GenericArray#componentType() componentType} attribute.
     * @param componentType The value for componentType 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> componentType(GenericType<T> componentType) {
      this.componentType = Objects.requireNonNull(componentType, "componentType");
      initBits &= ~INIT_BIT_COMPONENT_TYPE;
      return this;
    }

    /**
     * Adds one element to {@link GenericArray#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addValues(T element) {
      this.values.add(element);
      return this;
    }

    /**
     * Adds elements to {@link GenericArray#values() values} list.
     * @param elements An array of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    @SafeVarargs @SuppressWarnings("varargs")
    public final Builder<T> addValues(T... elements) {
      for (T element : elements) {
        this.values.add(element);
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link GenericArray#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> values(Iterable<? extends T> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link GenericArray#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAllValues(Iterable<? extends T> elements) {
      for (T element : elements) {
        this.values.add(element);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableGenericArray ImmutableGenericArray}.
     * @return An immutable instance of GenericArray
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableGenericArray<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableGenericArray<>(componentType, createUnmodifiableList(true, values));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COMPONENT_TYPE) != 0) attributes.add("componentType");
      return "Cannot build GenericArray, some of required attributes are not set " + attributes;
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
