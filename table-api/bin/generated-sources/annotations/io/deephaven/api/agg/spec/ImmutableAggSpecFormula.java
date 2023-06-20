package io.deephaven.api.agg.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecFormula}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecFormula.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAggSpecFormula.of()}.
 */
@Generated(from = "AggSpecFormula", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecFormula extends AggSpecFormula {
  private final String formula;
  private final String paramToken;

  private ImmutableAggSpecFormula(String formula, String paramToken) {
    this.formula = Objects.requireNonNull(formula, "formula");
    this.paramToken = Objects.requireNonNull(paramToken, "paramToken");
  }

  private ImmutableAggSpecFormula(ImmutableAggSpecFormula original, String formula, String paramToken) {
    this.formula = formula;
    this.paramToken = paramToken;
  }

  /**
   * The formula to use to calculate output values from grouped input values.
   * @return The formula
   */
  @Override
  public String formula() {
    return formula;
  }

  /**
   * The formula parameter token to be replaced with the input column name for evaluation.
   * 
   * @return The parameter token
   */
  @Override
  public String paramToken() {
    return paramToken;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecFormula#formula() formula} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for formula
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecFormula withFormula(String value) {
    String newValue = Objects.requireNonNull(value, "formula");
    if (this.formula.equals(newValue)) return this;
    return validate(new ImmutableAggSpecFormula(this, newValue, this.paramToken));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecFormula#paramToken() paramToken} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for paramToken
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecFormula withParamToken(String value) {
    String newValue = Objects.requireNonNull(value, "paramToken");
    if (this.paramToken.equals(newValue)) return this;
    return validate(new ImmutableAggSpecFormula(this, this.formula, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecFormula} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecFormula
        && equalTo(0, (ImmutableAggSpecFormula) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecFormula another) {
    return formula.equals(another.formula)
        && paramToken.equals(another.paramToken);
  }

  /**
   * Computes a hash code from attributes: {@code formula}, {@code paramToken}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + formula.hashCode();
    h += (h << 5) + paramToken.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecFormula} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecFormula{"
        + "formula=" + formula
        + ", paramToken=" + paramToken
        + "}";
  }

  /**
   * Construct a new immutable {@code AggSpecFormula} instance.
   * @param formula The value for the {@code formula} attribute
   * @param paramToken The value for the {@code paramToken} attribute
   * @return An immutable AggSpecFormula instance
   */
  public static ImmutableAggSpecFormula of(String formula, String paramToken) {
    return validate(new ImmutableAggSpecFormula(formula, paramToken));
  }

  private static ImmutableAggSpecFormula validate(ImmutableAggSpecFormula instance) {
    instance.checkParamToken();
    instance.checkFormula();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AggSpecFormula} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecFormula instance
   */
  public static ImmutableAggSpecFormula copyOf(AggSpecFormula instance) {
    if (instance instanceof ImmutableAggSpecFormula) {
      return (ImmutableAggSpecFormula) instance;
    }
    return ImmutableAggSpecFormula.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecFormula ImmutableAggSpecFormula}.
   * <pre>
   * ImmutableAggSpecFormula.builder()
   *    .formula(String) // required {@link AggSpecFormula#formula() formula}
   *    .paramToken(String) // required {@link AggSpecFormula#paramToken() paramToken}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecFormula builder
   */
  public static ImmutableAggSpecFormula.Builder builder() {
    return new ImmutableAggSpecFormula.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecFormula ImmutableAggSpecFormula}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecFormula", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_FORMULA = 0x1L;
    private static final long INIT_BIT_PARAM_TOKEN = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String formula;
    private @Nullable String paramToken;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecFormula} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecFormula instance) {
      Objects.requireNonNull(instance, "instance");
      formula(instance.formula());
      paramToken(instance.paramToken());
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecFormula#formula() formula} attribute.
     * @param formula The value for formula 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder formula(String formula) {
      this.formula = Objects.requireNonNull(formula, "formula");
      initBits &= ~INIT_BIT_FORMULA;
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecFormula#paramToken() paramToken} attribute.
     * @param paramToken The value for paramToken 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder paramToken(String paramToken) {
      this.paramToken = Objects.requireNonNull(paramToken, "paramToken");
      initBits &= ~INIT_BIT_PARAM_TOKEN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecFormula ImmutableAggSpecFormula}.
     * @return An immutable instance of AggSpecFormula
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecFormula build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableAggSpecFormula.validate(new ImmutableAggSpecFormula(null, formula, paramToken));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FORMULA) != 0) attributes.add("formula");
      if ((initBits & INIT_BIT_PARAM_TOKEN) != 0) attributes.add("paramToken");
      return "Cannot build AggSpecFormula, some of required attributes are not set " + attributes;
    }
  }
}
