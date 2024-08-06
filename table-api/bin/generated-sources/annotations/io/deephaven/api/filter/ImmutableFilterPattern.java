package io.deephaven.api.filter;

import io.deephaven.api.expression.Expression;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FilterPattern}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterPattern.builder()}.
 */
@Generated(from = "FilterPattern", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterPattern extends FilterPattern {
  private final Expression expression;
  private final Pattern pattern;
  private final FilterPattern.Mode mode;
  private final boolean invertPattern;

  private ImmutableFilterPattern(ImmutableFilterPattern.Builder builder) {
    this.expression = builder.expression;
    this.pattern = builder.pattern;
    this.mode = builder.mode;
    this.invertPattern = builder.invertPatternIsSet()
        ? builder.invertPattern
        : super.invertPattern();
  }

  private ImmutableFilterPattern(
      Expression expression,
      Pattern pattern,
      FilterPattern.Mode mode,
      boolean invertPattern) {
    this.expression = expression;
    this.pattern = pattern;
    this.mode = mode;
    this.invertPattern = invertPattern;
  }

  /**
   * @return The value of the {@code expression} attribute
   */
  @Override
  public Expression expression() {
    return expression;
  }

  /**
   * @return The value of the {@code pattern} attribute
   */
  @Override
  public Pattern pattern() {
    return pattern;
  }

  /**
   * @return The value of the {@code mode} attribute
   */
  @Override
  public FilterPattern.Mode mode() {
    return mode;
  }

  /**
   * @return The value of the {@code invertPattern} attribute
   */
  @Override
  public boolean invertPattern() {
    return invertPattern;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterPattern#expression() expression} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for expression
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterPattern withExpression(Expression value) {
    if (this.expression == value) return this;
    Expression newValue = Objects.requireNonNull(value, "expression");
    return new ImmutableFilterPattern(newValue, this.pattern, this.mode, this.invertPattern);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterPattern#pattern() pattern} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for pattern
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterPattern withPattern(Pattern value) {
    if (this.pattern == value) return this;
    Pattern newValue = Objects.requireNonNull(value, "pattern");
    return new ImmutableFilterPattern(this.expression, newValue, this.mode, this.invertPattern);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterPattern#mode() mode} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for mode
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterPattern withMode(FilterPattern.Mode value) {
    FilterPattern.Mode newValue = Objects.requireNonNull(value, "mode");
    if (this.mode == newValue) return this;
    return new ImmutableFilterPattern(this.expression, this.pattern, newValue, this.invertPattern);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterPattern#invertPattern() invertPattern} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for invertPattern
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterPattern withInvertPattern(boolean value) {
    if (this.invertPattern == value) return this;
    return new ImmutableFilterPattern(this.expression, this.pattern, this.mode, value);
  }

  /**
   * Creates an immutable copy of a {@link FilterPattern} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterPattern instance
   */
  public static ImmutableFilterPattern copyOf(FilterPattern instance) {
    if (instance instanceof ImmutableFilterPattern) {
      return (ImmutableFilterPattern) instance;
    }
    return ImmutableFilterPattern.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterPattern ImmutableFilterPattern}.
   * <pre>
   * ImmutableFilterPattern.builder()
   *    .expression(io.deephaven.api.expression.Expression) // required {@link FilterPattern#expression() expression}
   *    .pattern(regex.Pattern) // required {@link FilterPattern#pattern() pattern}
   *    .mode(io.deephaven.api.filter.FilterPattern.Mode) // required {@link FilterPattern#mode() mode}
   *    .invertPattern(boolean) // optional {@link FilterPattern#invertPattern() invertPattern}
   *    .build();
   * </pre>
   * @return A new ImmutableFilterPattern builder
   */
  public static ImmutableFilterPattern.Builder builder() {
    return new ImmutableFilterPattern.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterPattern ImmutableFilterPattern}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterPattern", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterPattern.Builder {
    private static final long INIT_BIT_EXPRESSION = 0x1L;
    private static final long INIT_BIT_PATTERN = 0x2L;
    private static final long INIT_BIT_MODE = 0x4L;
    private static final long OPT_BIT_INVERT_PATTERN = 0x1L;
    private long initBits = 0x7L;
    private long optBits;

    private @Nullable Expression expression;
    private @Nullable Pattern pattern;
    private @Nullable FilterPattern.Mode mode;
    private boolean invertPattern;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterPattern} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterPattern instance) {
      Objects.requireNonNull(instance, "instance");
      expression(instance.expression());
      pattern(instance.pattern());
      mode(instance.mode());
      invertPattern(instance.invertPattern());
      return this;
    }

    /**
     * Initializes the value for the {@link FilterPattern#expression() expression} attribute.
     * @param expression The value for expression 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder expression(Expression expression) {
      this.expression = Objects.requireNonNull(expression, "expression");
      initBits &= ~INIT_BIT_EXPRESSION;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterPattern#pattern() pattern} attribute.
     * @param pattern The value for pattern 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder pattern(Pattern pattern) {
      this.pattern = Objects.requireNonNull(pattern, "pattern");
      initBits &= ~INIT_BIT_PATTERN;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterPattern#mode() mode} attribute.
     * @param mode The value for mode 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder mode(FilterPattern.Mode mode) {
      this.mode = Objects.requireNonNull(mode, "mode");
      initBits &= ~INIT_BIT_MODE;
      return this;
    }

    /**
     * Initializes the value for the {@link FilterPattern#invertPattern() invertPattern} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FilterPattern#invertPattern() invertPattern}.</em>
     * @param invertPattern The value for invertPattern 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder invertPattern(boolean invertPattern) {
      this.invertPattern = invertPattern;
      optBits |= OPT_BIT_INVERT_PATTERN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFilterPattern ImmutableFilterPattern}.
     * @return An immutable instance of FilterPattern
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterPattern build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFilterPattern(this);
    }

    private boolean invertPatternIsSet() {
      return (optBits & OPT_BIT_INVERT_PATTERN) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_EXPRESSION) != 0) attributes.add("expression");
      if ((initBits & INIT_BIT_PATTERN) != 0) attributes.add("pattern");
      if ((initBits & INIT_BIT_MODE) != 0) attributes.add("mode");
      return "Cannot build FilterPattern, some of required attributes are not set " + attributes;
    }
  }
}
