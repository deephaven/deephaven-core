package io.deephaven.api.filter;

import io.deephaven.api.expression.Expression;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FilterComparison}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterComparison.builder()}.
 */
@Generated(from = "FilterComparison", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterComparison extends FilterComparison {
  private final FilterComparison.Operator operator;
  private final Expression lhs;
  private final Expression rhs;

  private ImmutableFilterComparison(
      FilterComparison.Operator operator,
      Expression lhs,
      Expression rhs) {
    this.operator = operator;
    this.lhs = lhs;
    this.rhs = rhs;
  }

  /**
   * The operator.
   * @return the operator
   */
  @Override
  public FilterComparison.Operator operator() {
    return operator;
  }

  /**
   * The left-hand side expression.
   * 
   * @return the left-hand side expression
   */
  @Override
  public Expression lhs() {
    return lhs;
  }

  /**
   * The right-hand side expression.
   * 
   * @return the right-hand side expression
   */
  @Override
  public Expression rhs() {
    return rhs;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterComparison#operator() operator} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for operator
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterComparison withOperator(FilterComparison.Operator value) {
    FilterComparison.Operator newValue = Objects.requireNonNull(value, "operator");
    if (this.operator == newValue) return this;
    return new ImmutableFilterComparison(newValue, this.lhs, this.rhs);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterComparison#lhs() lhs} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for lhs
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterComparison withLhs(Expression value) {
    if (this.lhs == value) return this;
    Expression newValue = Objects.requireNonNull(value, "lhs");
    return new ImmutableFilterComparison(this.operator, newValue, this.rhs);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterComparison#rhs() rhs} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rhs
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterComparison withRhs(Expression value) {
    if (this.rhs == value) return this;
    Expression newValue = Objects.requireNonNull(value, "rhs");
    return new ImmutableFilterComparison(this.operator, this.lhs, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterComparison} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterComparison
        && equalTo(0, (ImmutableFilterComparison) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterComparison another) {
    return operator.equals(another.operator)
        && lhs.equals(another.lhs)
        && rhs.equals(another.rhs);
  }

  /**
   * Computes a hash code from attributes: {@code operator}, {@code lhs}, {@code rhs}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + operator.hashCode();
    h += (h << 5) + lhs.hashCode();
    h += (h << 5) + rhs.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FilterComparison} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterComparison{"
        + "operator=" + operator
        + ", lhs=" + lhs
        + ", rhs=" + rhs
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link FilterComparison} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterComparison instance
   */
  public static ImmutableFilterComparison copyOf(FilterComparison instance) {
    if (instance instanceof ImmutableFilterComparison) {
      return (ImmutableFilterComparison) instance;
    }
    return ImmutableFilterComparison.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterComparison ImmutableFilterComparison}.
   * <pre>
   * ImmutableFilterComparison.builder()
   *    .operator(io.deephaven.api.filter.FilterComparison.Operator) // required {@link FilterComparison#operator() operator}
   *    .lhs(io.deephaven.api.expression.Expression) // required {@link FilterComparison#lhs() lhs}
   *    .rhs(io.deephaven.api.expression.Expression) // required {@link FilterComparison#rhs() rhs}
   *    .build();
   * </pre>
   * @return A new ImmutableFilterComparison builder
   */
  public static ImmutableFilterComparison.Builder builder() {
    return new ImmutableFilterComparison.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterComparison ImmutableFilterComparison}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterComparison", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterComparison.Builder {
    private static final long INIT_BIT_OPERATOR = 0x1L;
    private static final long INIT_BIT_LHS = 0x2L;
    private static final long INIT_BIT_RHS = 0x4L;
    private long initBits = 0x7L;

    private @Nullable FilterComparison.Operator operator;
    private @Nullable Expression lhs;
    private @Nullable Expression rhs;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterComparison} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterComparison instance) {
      Objects.requireNonNull(instance, "instance");
      operator(instance.operator());
      lhs(instance.lhs());
      rhs(instance.rhs());
      return this;
    }

    /**
     * Initializes the value for the {@link FilterComparison#operator() operator} attribute.
     * @param operator The value for operator 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder operator(FilterComparison.Operator operator) {
      this.operator = Objects.requireNonNull(operator, "operator");
      initBits &= ~INIT_BIT_OPERATOR;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterComparison#lhs() lhs} attribute.
     * @param lhs The value for lhs 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder lhs(Expression lhs) {
      this.lhs = Objects.requireNonNull(lhs, "lhs");
      initBits &= ~INIT_BIT_LHS;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterComparison#rhs() rhs} attribute.
     * @param rhs The value for rhs 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rhs(Expression rhs) {
      this.rhs = Objects.requireNonNull(rhs, "rhs");
      initBits &= ~INIT_BIT_RHS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFilterComparison ImmutableFilterComparison}.
     * @return An immutable instance of FilterComparison
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterComparison build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFilterComparison(operator, lhs, rhs);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_OPERATOR) != 0) attributes.add("operator");
      if ((initBits & INIT_BIT_LHS) != 0) attributes.add("lhs");
      if ((initBits & INIT_BIT_RHS) != 0) attributes.add("rhs");
      return "Cannot build FilterComparison, some of required attributes are not set " + attributes;
    }
  }
}
