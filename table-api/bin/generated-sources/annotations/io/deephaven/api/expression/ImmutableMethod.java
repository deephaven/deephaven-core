package io.deephaven.api.expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Method}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMethod.builder()}.
 */
@Generated(from = "Method", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableMethod extends Method {
  private final Expression object;
  private final String name;
  private final List<Expression> arguments;

  private ImmutableMethod(
      Expression object,
      String name,
      List<Expression> arguments) {
    this.object = object;
    this.name = name;
    this.arguments = arguments;
  }

  /**
   * The method object.
   * @return the method object
   */
  @Override
  public Expression object() {
    return object;
  }

  /**
   * The method name.
   * @return the method name
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The method arguments.
   * @return the method arguments
   */
  @Override
  public List<Expression> arguments() {
    return arguments;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Method#object() object} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for object
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableMethod withObject(Expression value) {
    if (this.object == value) return this;
    Expression newValue = Objects.requireNonNull(value, "object");
    return new ImmutableMethod(newValue, this.name, this.arguments);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Method#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableMethod withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new ImmutableMethod(this.object, newValue, this.arguments);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Method#arguments() arguments}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMethod withArguments(Expression... elements) {
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableMethod(this.object, this.name, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Method#arguments() arguments}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of arguments elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMethod withArguments(Iterable<? extends Expression> elements) {
    if (this.arguments == elements) return this;
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableMethod(this.object, this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMethod} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMethod
        && equalTo(0, (ImmutableMethod) another);
  }

  private boolean equalTo(int synthetic, ImmutableMethod another) {
    return object.equals(another.object)
        && name.equals(another.name)
        && arguments.equals(another.arguments);
  }

  /**
   * Computes a hash code from attributes: {@code object}, {@code name}, {@code arguments}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + object.hashCode();
    h += (h << 5) + name.hashCode();
    h += (h << 5) + arguments.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Method} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Method{"
        + "object=" + object
        + ", name=" + name
        + ", arguments=" + arguments
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link Method} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Method instance
   */
  public static ImmutableMethod copyOf(Method instance) {
    if (instance instanceof ImmutableMethod) {
      return (ImmutableMethod) instance;
    }
    return ImmutableMethod.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableMethod ImmutableMethod}.
   * <pre>
   * ImmutableMethod.builder()
   *    .object(io.deephaven.api.expression.Expression) // required {@link Method#object() object}
   *    .name(String) // required {@link Method#name() name}
   *    .addArguments|addAllArguments(Expression) // {@link Method#arguments() arguments} elements
   *    .build();
   * </pre>
   * @return A new ImmutableMethod builder
   */
  public static ImmutableMethod.Builder builder() {
    return new ImmutableMethod.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMethod ImmutableMethod}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Method", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Method.Builder {
    private static final long INIT_BIT_OBJECT = 0x1L;
    private static final long INIT_BIT_NAME = 0x2L;
    private long initBits = 0x3L;

    private @Nullable Expression object;
    private @Nullable String name;
    private List<Expression> arguments = new ArrayList<Expression>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Method} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Method instance) {
      Objects.requireNonNull(instance, "instance");
      object(instance.object());
      name(instance.name());
      addAllArguments(instance.arguments());
      return this;
    }

    /**
     * Initializes the value for the {@link Method#object() object} attribute.
     * @param object The value for object 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder object(Expression object) {
      this.object = Objects.requireNonNull(object, "object");
      initBits &= ~INIT_BIT_OBJECT;
      return this;
    }

    /**
     * Initializes the value for the {@link Method#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Adds one element to {@link Method#arguments() arguments} list.
     * @param element A arguments element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addArguments(Expression element) {
      this.arguments.add(Objects.requireNonNull(element, "arguments element"));
      return this;
    }

    /**
     * Adds elements to {@link Method#arguments() arguments} list.
     * @param elements An array of arguments elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addArguments(Expression... elements) {
      for (Expression element : elements) {
        this.arguments.add(Objects.requireNonNull(element, "arguments element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link Method#arguments() arguments} list.
     * @param elements An iterable of arguments elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder arguments(Iterable<? extends Expression> elements) {
      this.arguments.clear();
      return addAllArguments(elements);
    }

    /**
     * Adds elements to {@link Method#arguments() arguments} list.
     * @param elements An iterable of arguments elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllArguments(Iterable<? extends Expression> elements) {
      for (Expression element : elements) {
        this.arguments.add(Objects.requireNonNull(element, "arguments element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableMethod ImmutableMethod}.
     * @return An immutable instance of Method
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMethod build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableMethod(object, name, createUnmodifiableList(true, arguments));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_OBJECT) != 0) attributes.add("object");
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      return "Cannot build Method, some of required attributes are not set " + attributes;
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
