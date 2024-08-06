package io.deephaven.api.filter;

import io.deephaven.api.expression.Expression;
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
 * Immutable implementation of {@link FilterIn}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterIn.builder()}.
 */
@Generated(from = "FilterIn", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterIn extends FilterIn {
  private final Expression expression;
  private final List<Expression> values;

  private ImmutableFilterIn(
      Expression expression,
      List<Expression> values) {
    this.expression = expression;
    this.values = values;
  }

  /**
   * @return The value of the {@code expression} attribute
   */
  @Override
  public Expression expression() {
    return expression;
  }

  /**
   * @return The value of the {@code values} attribute
   */
  @Override
  public List<Expression> values() {
    return values;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterIn#expression() expression} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for expression
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterIn withExpression(Expression value) {
    if (this.expression == value) return this;
    Expression newValue = Objects.requireNonNull(value, "expression");
    return validate(new ImmutableFilterIn(newValue, this.values));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FilterIn#values() values}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterIn withValues(Expression... elements) {
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableFilterIn(this.expression, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FilterIn#values() values}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of values elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterIn withValues(Iterable<? extends Expression> elements) {
    if (this.values == elements) return this;
    List<Expression> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableFilterIn(this.expression, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterIn} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterIn
        && equalTo(0, (ImmutableFilterIn) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterIn another) {
    return expression.equals(another.expression)
        && values.equals(another.values);
  }

  /**
   * Computes a hash code from attributes: {@code expression}, {@code values}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + expression.hashCode();
    h += (h << 5) + values.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FilterIn} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterIn{"
        + "expression=" + expression
        + ", values=" + values
        + "}";
  }

  private static ImmutableFilterIn validate(ImmutableFilterIn instance) {
    instance.checkNotEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link FilterIn} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterIn instance
   */
  public static ImmutableFilterIn copyOf(FilterIn instance) {
    if (instance instanceof ImmutableFilterIn) {
      return (ImmutableFilterIn) instance;
    }
    return ImmutableFilterIn.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterIn ImmutableFilterIn}.
   * <pre>
   * ImmutableFilterIn.builder()
   *    .expression(io.deephaven.api.expression.Expression) // required {@link FilterIn#expression() expression}
   *    .addValues|addAllValues(io.deephaven.api.expression.Expression) // {@link FilterIn#values() values} elements
   *    .build();
   * </pre>
   * @return A new ImmutableFilterIn builder
   */
  public static ImmutableFilterIn.Builder builder() {
    return new ImmutableFilterIn.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterIn ImmutableFilterIn}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterIn", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterIn.Builder {
    private static final long INIT_BIT_EXPRESSION = 0x1L;
    private long initBits = 0x1L;

    private @Nullable Expression expression;
    private List<Expression> values = new ArrayList<Expression>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterIn} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterIn instance) {
      Objects.requireNonNull(instance, "instance");
      expression(instance.expression());
      addAllValues(instance.values());
      return this;
    }

    /**
     * Initializes the value for the {@link FilterIn#expression() expression} attribute.
     * @param expression The value for expression 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder expression(Expression expression) {
      this.expression = Objects.requireNonNull(expression, "expression");
      initBits &= ~INIT_BIT_EXPRESSION;
      return this;
    }

    /**
     * Adds one element to {@link FilterIn#values() values} list.
     * @param element A values element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Expression element) {
      this.values.add(Objects.requireNonNull(element, "values element"));
      return this;
    }

    /**
     * Adds elements to {@link FilterIn#values() values} list.
     * @param elements An array of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addValues(Expression... elements) {
      for (Expression element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link FilterIn#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder values(Iterable<? extends Expression> elements) {
      this.values.clear();
      return addAllValues(elements);
    }

    /**
     * Adds elements to {@link FilterIn#values() values} list.
     * @param elements An iterable of values elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllValues(Iterable<? extends Expression> elements) {
      for (Expression element : elements) {
        this.values.add(Objects.requireNonNull(element, "values element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableFilterIn ImmutableFilterIn}.
     * @return An immutable instance of FilterIn
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterIn build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableFilterIn.validate(new ImmutableFilterIn(expression, createUnmodifiableList(true, values)));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_EXPRESSION) != 0) attributes.add("expression");
      return "Cannot build FilterIn, some of required attributes are not set " + attributes;
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
