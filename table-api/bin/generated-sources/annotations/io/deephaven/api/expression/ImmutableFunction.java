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
 * Immutable implementation of {@link Function}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFunction.builder()}.
 */
@Generated(from = "Function", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFunction extends Function {
  private final String name;
  private final List<Expression> arguments;

  private ImmutableFunction(String name, List<Expression> arguments) {
    this.name = name;
    this.arguments = arguments;
  }

  /**
   * The function name.
   * @return the name
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The function arguments.
   * @return the arguments
   */
  @Override
  public List<Expression> arguments() {
    return arguments;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Function#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFunction withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return new ImmutableFunction(newValue, this.arguments);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Function#arguments() arguments}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFunction withArguments(Expression... elements) {
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableFunction(this.name, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Function#arguments() arguments}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of arguments elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFunction withArguments(Iterable<? extends Expression> elements) {
    if (this.arguments == elements) return this;
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableFunction(this.name, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFunction} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFunction
        && equalTo(0, (ImmutableFunction) another);
  }

  private boolean equalTo(int synthetic, ImmutableFunction another) {
    return name.equals(another.name)
        && arguments.equals(another.arguments);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code arguments}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + arguments.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Function} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Function{"
        + "name=" + name
        + ", arguments=" + arguments
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link Function} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Function instance
   */
  public static ImmutableFunction copyOf(Function instance) {
    if (instance instanceof ImmutableFunction) {
      return (ImmutableFunction) instance;
    }
    return ImmutableFunction.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFunction ImmutableFunction}.
   * <pre>
   * ImmutableFunction.builder()
   *    .name(String) // required {@link Function#name() name}
   *    .addArguments|addAllArguments(io.deephaven.api.expression.Expression) // {@link Function#arguments() arguments} elements
   *    .build();
   * </pre>
   * @return A new ImmutableFunction builder
   */
  public static ImmutableFunction.Builder builder() {
    return new ImmutableFunction.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFunction ImmutableFunction}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Function", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Function.Builder {
    private static final long INIT_BIT_NAME = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String name;
    private List<Expression> arguments = new ArrayList<Expression>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Function} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Function instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      addAllArguments(instance.arguments());
      return this;
    }

    /**
     * Initializes the value for the {@link Function#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Adds one element to {@link Function#arguments() arguments} list.
     * @param element A arguments element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addArguments(Expression element) {
      this.arguments.add(Objects.requireNonNull(element, "arguments element"));
      return this;
    }

    /**
     * Adds elements to {@link Function#arguments() arguments} list.
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
     * Sets or replaces all elements for {@link Function#arguments() arguments} list.
     * @param elements An iterable of arguments elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder arguments(Iterable<? extends Expression> elements) {
      this.arguments.clear();
      return addAllArguments(elements);
    }

    /**
     * Adds elements to {@link Function#arguments() arguments} list.
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
     * Builds a new {@link ImmutableFunction ImmutableFunction}.
     * @return An immutable instance of Function
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFunction build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFunction(name, createUnmodifiableList(true, arguments));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      return "Cannot build Function, some of required attributes are not set " + attributes;
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
