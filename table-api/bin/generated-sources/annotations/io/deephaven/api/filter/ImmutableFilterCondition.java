package io.deephaven.api.filter;

import io.deephaven.api.value.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FilterCondition}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterCondition.builder()}.
 */
@Generated(from = "FilterCondition", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterCondition extends FilterCondition {
  private final FilterCondition.Operator operator;
  private final Value lhs;
  private final Value rhs;

  private ImmutableFilterCondition(
      FilterCondition.Operator operator,
      Value lhs,
      Value rhs) {
    this.operator = operator;
    this.lhs = lhs;
    this.rhs = rhs;
  }

  /**
   * The operator.
   * @return the operator
   */
  @Override
  public FilterCondition.Operator operator() {
    return operator;
  }

  /**
   * The left-hand side value.
   * 
   * @return the left-hand side value
   */
  @Override
  public Value lhs() {
    return lhs;
  }

  /**
   * The right-hand side value.
   * 
   * @return the right-hand side value
   */
  @Override
  public Value rhs() {
    return rhs;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterCondition#operator() operator} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for operator
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterCondition withOperator(FilterCondition.Operator value) {
    FilterCondition.Operator newValue = Objects.requireNonNull(value, "operator");
    if (this.operator == newValue) return this;
    return new ImmutableFilterCondition(newValue, this.lhs, this.rhs);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterCondition#lhs() lhs} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for lhs
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterCondition withLhs(Value value) {
    if (this.lhs == value) return this;
    Value newValue = Objects.requireNonNull(value, "lhs");
    return new ImmutableFilterCondition(this.operator, newValue, this.rhs);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterCondition#rhs() rhs} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rhs
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterCondition withRhs(Value value) {
    if (this.rhs == value) return this;
    Value newValue = Objects.requireNonNull(value, "rhs");
    return new ImmutableFilterCondition(this.operator, this.lhs, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterCondition} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterCondition
        && equalTo(0, (ImmutableFilterCondition) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterCondition another) {
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
   * Prints the immutable value {@code FilterCondition} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterCondition{"
        + "operator=" + operator
        + ", lhs=" + lhs
        + ", rhs=" + rhs
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link FilterCondition} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterCondition instance
   */
  public static ImmutableFilterCondition copyOf(FilterCondition instance) {
    if (instance instanceof ImmutableFilterCondition) {
      return (ImmutableFilterCondition) instance;
    }
    return ImmutableFilterCondition.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterCondition ImmutableFilterCondition}.
   * <pre>
   * ImmutableFilterCondition.builder()
   *    .operator(io.deephaven.api.filter.FilterCondition.Operator) // required {@link FilterCondition#operator() operator}
   *    .lhs(io.deephaven.api.value.Value) // required {@link FilterCondition#lhs() lhs}
   *    .rhs(io.deephaven.api.value.Value) // required {@link FilterCondition#rhs() rhs}
   *    .build();
   * </pre>
   * @return A new ImmutableFilterCondition builder
   */
  public static ImmutableFilterCondition.Builder builder() {
    return new ImmutableFilterCondition.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterCondition ImmutableFilterCondition}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterCondition", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterCondition.Builder {
    private static final long INIT_BIT_OPERATOR = 0x1L;
    private static final long INIT_BIT_LHS = 0x2L;
    private static final long INIT_BIT_RHS = 0x4L;
    private long initBits = 0x7L;

    private @Nullable FilterCondition.Operator operator;
    private @Nullable Value lhs;
    private @Nullable Value rhs;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterCondition} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterCondition instance) {
      Objects.requireNonNull(instance, "instance");
      operator(instance.operator());
      lhs(instance.lhs());
      rhs(instance.rhs());
      return this;
    }

    /**
     * Initializes the value for the {@link FilterCondition#operator() operator} attribute.
     * @param operator The value for operator 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder operator(FilterCondition.Operator operator) {
      this.operator = Objects.requireNonNull(operator, "operator");
      initBits &= ~INIT_BIT_OPERATOR;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterCondition#lhs() lhs} attribute.
     * @param lhs The value for lhs 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder lhs(Value lhs) {
      this.lhs = Objects.requireNonNull(lhs, "lhs");
      initBits &= ~INIT_BIT_LHS;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterCondition#rhs() rhs} attribute.
     * @param rhs The value for rhs 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rhs(Value rhs) {
      this.rhs = Objects.requireNonNull(rhs, "rhs");
      initBits &= ~INIT_BIT_RHS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFilterCondition ImmutableFilterCondition}.
     * @return An immutable instance of FilterCondition
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterCondition build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFilterCondition(operator, lhs, rhs);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_OPERATOR) != 0) attributes.add("operator");
      if ((initBits & INIT_BIT_LHS) != 0) attributes.add("lhs");
      if ((initBits & INIT_BIT_RHS) != 0) attributes.add("rhs");
      return "Cannot build FilterCondition, some of required attributes are not set " + attributes;
    }
  }
}
