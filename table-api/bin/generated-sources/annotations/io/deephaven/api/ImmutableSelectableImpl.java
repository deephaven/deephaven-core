package io.deephaven.api;

import io.deephaven.api.expression.Expression;
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
 * Immutable implementation of {@link SelectableImpl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSelectableImpl.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSelectableImpl.of()}.
 */
@Generated(from = "SelectableImpl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutableSelectableImpl extends SelectableImpl {
  private final ColumnName newColumn;
  private final Expression expression;

  private ImmutableSelectableImpl(ColumnName newColumn, Expression expression) {
    this.newColumn = Objects.requireNonNull(newColumn, "newColumn");
    this.expression = Objects.requireNonNull(expression, "expression");
  }

  private ImmutableSelectableImpl(
      ImmutableSelectableImpl original,
      ColumnName newColumn,
      Expression expression) {
    this.newColumn = newColumn;
    this.expression = expression;
  }

  /**
   * @return The value of the {@code newColumn} attribute
   */
  @Override
  public ColumnName newColumn() {
    return newColumn;
  }

  /**
   * @return The value of the {@code expression} attribute
   */
  @Override
  public Expression expression() {
    return expression;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SelectableImpl#newColumn() newColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for newColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSelectableImpl withNewColumn(ColumnName value) {
    if (this.newColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "newColumn");
    return validate(new ImmutableSelectableImpl(this, newValue, this.expression));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SelectableImpl#expression() expression} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for expression
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSelectableImpl withExpression(Expression value) {
    if (this.expression == value) return this;
    Expression newValue = Objects.requireNonNull(value, "expression");
    return validate(new ImmutableSelectableImpl(this, this.newColumn, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSelectableImpl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSelectableImpl
        && equalTo(0, (ImmutableSelectableImpl) another);
  }

  private boolean equalTo(int synthetic, ImmutableSelectableImpl another) {
    return newColumn.equals(another.newColumn)
        && expression.equals(another.expression);
  }

  /**
   * Computes a hash code from attributes: {@code newColumn}, {@code expression}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + newColumn.hashCode();
    h += (h << 5) + expression.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SelectableImpl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SelectableImpl{"
        + "newColumn=" + newColumn
        + ", expression=" + expression
        + "}";
  }

  /**
   * Construct a new immutable {@code SelectableImpl} instance.
   * @param newColumn The value for the {@code newColumn} attribute
   * @param expression The value for the {@code expression} attribute
   * @return An immutable SelectableImpl instance
   */
  public static ImmutableSelectableImpl of(ColumnName newColumn, Expression expression) {
    return validate(new ImmutableSelectableImpl(newColumn, expression));
  }

  private static ImmutableSelectableImpl validate(ImmutableSelectableImpl instance) {
    instance.checkExpressionNotSameColumn();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link SelectableImpl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SelectableImpl instance
   */
  public static ImmutableSelectableImpl copyOf(SelectableImpl instance) {
    if (instance instanceof ImmutableSelectableImpl) {
      return (ImmutableSelectableImpl) instance;
    }
    return ImmutableSelectableImpl.builder()
        .from(instance)
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableSelectableImpl ImmutableSelectableImpl}.
   * <pre>
   * ImmutableSelectableImpl.builder()
   *    .newColumn(io.deephaven.api.ColumnName) // required {@link SelectableImpl#newColumn() newColumn}
   *    .expression(io.deephaven.api.expression.Expression) // required {@link SelectableImpl#expression() expression}
   *    .build();
   * </pre>
   * @return A new ImmutableSelectableImpl builder
   */
  public static ImmutableSelectableImpl.Builder builder() {
    return new ImmutableSelectableImpl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSelectableImpl ImmutableSelectableImpl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SelectableImpl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_NEW_COLUMN = 0x1L;
    private static final long INIT_BIT_EXPRESSION = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ColumnName newColumn;
    private @Nullable Expression expression;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.Selectable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Selectable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.SelectableImpl} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SelectableImpl instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof Selectable) {
        Selectable instance = (Selectable) object;
        if ((bits & 0x1L) == 0) {
          newColumn(instance.newColumn());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          expression(instance.expression());
          bits |= 0x2L;
        }
      }
      if (object instanceof SelectableImpl) {
        SelectableImpl instance = (SelectableImpl) object;
        if ((bits & 0x1L) == 0) {
          newColumn(instance.newColumn());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          expression(instance.expression());
          bits |= 0x2L;
        }
      }
    }

    /**
     * Initializes the value for the {@link SelectableImpl#newColumn() newColumn} attribute.
     * @param newColumn The value for newColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder newColumn(ColumnName newColumn) {
      this.newColumn = Objects.requireNonNull(newColumn, "newColumn");
      initBits &= ~INIT_BIT_NEW_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link SelectableImpl#expression() expression} attribute.
     * @param expression The value for expression 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder expression(Expression expression) {
      this.expression = Objects.requireNonNull(expression, "expression");
      initBits &= ~INIT_BIT_EXPRESSION;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSelectableImpl ImmutableSelectableImpl}.
     * @return An immutable instance of SelectableImpl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSelectableImpl build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableSelectableImpl.validate(new ImmutableSelectableImpl(null, newColumn, expression));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NEW_COLUMN) != 0) attributes.add("newColumn");
      if ((initBits & INIT_BIT_EXPRESSION) != 0) attributes.add("expression");
      return "Cannot build SelectableImpl, some of required attributes are not set " + attributes;
    }
  }
}
