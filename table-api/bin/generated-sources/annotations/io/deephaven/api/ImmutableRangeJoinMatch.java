package io.deephaven.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;
<<<<<<< HEAD
=======
import static io.deephaven.api.RangeEndRule.*;
import static io.deephaven.api.RangeStartRule.*;
>>>>>>> main

/**
 * Immutable implementation of {@link RangeJoinMatch}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRangeJoinMatch.builder()}.
 */
@Generated(from = "RangeJoinMatch", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRangeJoinMatch extends RangeJoinMatch {
  private final ColumnName leftStartColumn;
  private final RangeStartRule rangeStartRule;
  private final ColumnName rightRangeColumn;
  private final RangeEndRule rangeEndRule;
  private final ColumnName leftEndColumn;

  private ImmutableRangeJoinMatch(
      ColumnName leftStartColumn,
      RangeStartRule rangeStartRule,
      ColumnName rightRangeColumn,
      RangeEndRule rangeEndRule,
      ColumnName leftEndColumn) {
    this.leftStartColumn = leftStartColumn;
    this.rangeStartRule = rangeStartRule;
    this.rightRangeColumn = rightRangeColumn;
    this.rangeEndRule = rangeEndRule;
    this.leftEndColumn = leftEndColumn;
  }

  /**
   * The column from the left table that bounds the start of the responsive range from the right table.
   * @return The left start column name
   */
  @Override
  public ColumnName leftStartColumn() {
    return leftStartColumn;
  }

  /**
   * The rule applied to {@link #leftStartColumn()} and {@link #rightRangeColumn()} to determine the start of the
   * responsive range from the right table for a given left table row.
   * 
   * @return The range start rule
   */
  @Override
  public RangeStartRule rangeStartRule() {
    return rangeStartRule;
  }

  /**
   * The column name from the right table that determines which right table rows are responsive to a given left table
   * row.
   * 
   * @return The right range column name
   */
  @Override
  public ColumnName rightRangeColumn() {
    return rightRangeColumn;
  }

  /**
   * The rule applied to {@link #leftStartColumn()} and {@link #rightRangeColumn()} to determine the end of the
   * responsive range from the right table for a given left table row.
   * 
   * @return The range end rule
   */
  @Override
  public RangeEndRule rangeEndRule() {
    return rangeEndRule;
  }

  /**
   * The column from the left table that bounds the end of the responsive range from the right table.
   * @return The left end column name
   */
  @Override
  public ColumnName leftEndColumn() {
    return leftEndColumn;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinMatch#leftStartColumn() leftStartColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for leftStartColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinMatch withLeftStartColumn(ColumnName value) {
    if (this.leftStartColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "leftStartColumn");
    return validate(new ImmutableRangeJoinMatch(newValue, this.rangeStartRule, this.rightRangeColumn, this.rangeEndRule, this.leftEndColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinMatch#rangeStartRule() rangeStartRule} attribute.
<<<<<<< HEAD
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
=======
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
>>>>>>> main
   * @param value A new value for rangeStartRule
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinMatch withRangeStartRule(RangeStartRule value) {
<<<<<<< HEAD
    RangeStartRule newValue = Objects.requireNonNull(value, "rangeStartRule");
    if (this.rangeStartRule == newValue) return this;
=======
    if (this.rangeStartRule == value) return this;
    RangeStartRule newValue = Objects.requireNonNull(value, "rangeStartRule");
>>>>>>> main
    return validate(new ImmutableRangeJoinMatch(this.leftStartColumn, newValue, this.rightRangeColumn, this.rangeEndRule, this.leftEndColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinMatch#rightRangeColumn() rightRangeColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rightRangeColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinMatch withRightRangeColumn(ColumnName value) {
    if (this.rightRangeColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "rightRangeColumn");
    return validate(new ImmutableRangeJoinMatch(this.leftStartColumn, this.rangeStartRule, newValue, this.rangeEndRule, this.leftEndColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinMatch#rangeEndRule() rangeEndRule} attribute.
<<<<<<< HEAD
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
=======
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
>>>>>>> main
   * @param value A new value for rangeEndRule
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinMatch withRangeEndRule(RangeEndRule value) {
<<<<<<< HEAD
    RangeEndRule newValue = Objects.requireNonNull(value, "rangeEndRule");
    if (this.rangeEndRule == newValue) return this;
=======
    if (this.rangeEndRule == value) return this;
    RangeEndRule newValue = Objects.requireNonNull(value, "rangeEndRule");
>>>>>>> main
    return validate(new ImmutableRangeJoinMatch(this.leftStartColumn, this.rangeStartRule, this.rightRangeColumn, newValue, this.leftEndColumn));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinMatch#leftEndColumn() leftEndColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for leftEndColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinMatch withLeftEndColumn(ColumnName value) {
    if (this.leftEndColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "leftEndColumn");
    return validate(new ImmutableRangeJoinMatch(this.leftStartColumn, this.rangeStartRule, this.rightRangeColumn, this.rangeEndRule, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRangeJoinMatch} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRangeJoinMatch
        && equalTo(0, (ImmutableRangeJoinMatch) another);
  }

  private boolean equalTo(int synthetic, ImmutableRangeJoinMatch another) {
    return leftStartColumn.equals(another.leftStartColumn)
        && rangeStartRule.equals(another.rangeStartRule)
        && rightRangeColumn.equals(another.rightRangeColumn)
        && rangeEndRule.equals(another.rangeEndRule)
        && leftEndColumn.equals(another.leftEndColumn);
  }

  /**
   * Computes a hash code from attributes: {@code leftStartColumn}, {@code rangeStartRule}, {@code rightRangeColumn}, {@code rangeEndRule}, {@code leftEndColumn}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + leftStartColumn.hashCode();
    h += (h << 5) + rangeStartRule.hashCode();
    h += (h << 5) + rightRangeColumn.hashCode();
    h += (h << 5) + rangeEndRule.hashCode();
    h += (h << 5) + leftEndColumn.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RangeJoinMatch} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RangeJoinMatch{"
        + "leftStartColumn=" + leftStartColumn
        + ", rangeStartRule=" + rangeStartRule
        + ", rightRangeColumn=" + rightRangeColumn
        + ", rangeEndRule=" + rangeEndRule
        + ", leftEndColumn=" + leftEndColumn
        + "}";
  }

  private static ImmutableRangeJoinMatch validate(ImmutableRangeJoinMatch instance) {
    instance.checkLeftColumnsDifferent();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link RangeJoinMatch} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RangeJoinMatch instance
   */
  public static ImmutableRangeJoinMatch copyOf(RangeJoinMatch instance) {
    if (instance instanceof ImmutableRangeJoinMatch) {
      return (ImmutableRangeJoinMatch) instance;
    }
    return ImmutableRangeJoinMatch.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRangeJoinMatch ImmutableRangeJoinMatch}.
   * <pre>
   * ImmutableRangeJoinMatch.builder()
   *    .leftStartColumn(io.deephaven.api.ColumnName) // required {@link RangeJoinMatch#leftStartColumn() leftStartColumn}
<<<<<<< HEAD
   *    .rangeStartRule(io.deephaven.api.RangeStartRule) // required {@link RangeJoinMatch#rangeStartRule() rangeStartRule}
   *    .rightRangeColumn(io.deephaven.api.ColumnName) // required {@link RangeJoinMatch#rightRangeColumn() rightRangeColumn}
   *    .rangeEndRule(io.deephaven.api.RangeEndRule) // required {@link RangeJoinMatch#rangeEndRule() rangeEndRule}
=======
   *    .rangeStartRule(RangeStartRule) // required {@link RangeJoinMatch#rangeStartRule() rangeStartRule}
   *    .rightRangeColumn(io.deephaven.api.ColumnName) // required {@link RangeJoinMatch#rightRangeColumn() rightRangeColumn}
   *    .rangeEndRule(RangeEndRule) // required {@link RangeJoinMatch#rangeEndRule() rangeEndRule}
>>>>>>> main
   *    .leftEndColumn(io.deephaven.api.ColumnName) // required {@link RangeJoinMatch#leftEndColumn() leftEndColumn}
   *    .build();
   * </pre>
   * @return A new ImmutableRangeJoinMatch builder
   */
  public static ImmutableRangeJoinMatch.Builder builder() {
    return new ImmutableRangeJoinMatch.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRangeJoinMatch ImmutableRangeJoinMatch}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RangeJoinMatch", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements RangeJoinMatch.Builder {
    private static final long INIT_BIT_LEFT_START_COLUMN = 0x1L;
    private static final long INIT_BIT_RANGE_START_RULE = 0x2L;
    private static final long INIT_BIT_RIGHT_RANGE_COLUMN = 0x4L;
    private static final long INIT_BIT_RANGE_END_RULE = 0x8L;
    private static final long INIT_BIT_LEFT_END_COLUMN = 0x10L;
    private long initBits = 0x1fL;

    private @Nullable ColumnName leftStartColumn;
    private @Nullable RangeStartRule rangeStartRule;
    private @Nullable ColumnName rightRangeColumn;
    private @Nullable RangeEndRule rangeEndRule;
    private @Nullable ColumnName leftEndColumn;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RangeJoinMatch} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RangeJoinMatch instance) {
      Objects.requireNonNull(instance, "instance");
      leftStartColumn(instance.leftStartColumn());
      rangeStartRule(instance.rangeStartRule());
      rightRangeColumn(instance.rightRangeColumn());
      rangeEndRule(instance.rangeEndRule());
      leftEndColumn(instance.leftEndColumn());
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinMatch#leftStartColumn() leftStartColumn} attribute.
     * @param leftStartColumn The value for leftStartColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder leftStartColumn(ColumnName leftStartColumn) {
      this.leftStartColumn = Objects.requireNonNull(leftStartColumn, "leftStartColumn");
      initBits &= ~INIT_BIT_LEFT_START_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinMatch#rangeStartRule() rangeStartRule} attribute.
     * @param rangeStartRule The value for rangeStartRule 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rangeStartRule(RangeStartRule rangeStartRule) {
      this.rangeStartRule = Objects.requireNonNull(rangeStartRule, "rangeStartRule");
      initBits &= ~INIT_BIT_RANGE_START_RULE;
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinMatch#rightRangeColumn() rightRangeColumn} attribute.
     * @param rightRangeColumn The value for rightRangeColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rightRangeColumn(ColumnName rightRangeColumn) {
      this.rightRangeColumn = Objects.requireNonNull(rightRangeColumn, "rightRangeColumn");
      initBits &= ~INIT_BIT_RIGHT_RANGE_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinMatch#rangeEndRule() rangeEndRule} attribute.
     * @param rangeEndRule The value for rangeEndRule 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rangeEndRule(RangeEndRule rangeEndRule) {
      this.rangeEndRule = Objects.requireNonNull(rangeEndRule, "rangeEndRule");
      initBits &= ~INIT_BIT_RANGE_END_RULE;
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinMatch#leftEndColumn() leftEndColumn} attribute.
     * @param leftEndColumn The value for leftEndColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder leftEndColumn(ColumnName leftEndColumn) {
      this.leftEndColumn = Objects.requireNonNull(leftEndColumn, "leftEndColumn");
      initBits &= ~INIT_BIT_LEFT_END_COLUMN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRangeJoinMatch ImmutableRangeJoinMatch}.
     * @return An immutable instance of RangeJoinMatch
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRangeJoinMatch build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableRangeJoinMatch.validate(new ImmutableRangeJoinMatch(leftStartColumn, rangeStartRule, rightRangeColumn, rangeEndRule, leftEndColumn));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT_START_COLUMN) != 0) attributes.add("leftStartColumn");
      if ((initBits & INIT_BIT_RANGE_START_RULE) != 0) attributes.add("rangeStartRule");
      if ((initBits & INIT_BIT_RIGHT_RANGE_COLUMN) != 0) attributes.add("rightRangeColumn");
      if ((initBits & INIT_BIT_RANGE_END_RULE) != 0) attributes.add("rangeEndRule");
      if ((initBits & INIT_BIT_LEFT_END_COLUMN) != 0) attributes.add("leftEndColumn");
      return "Cannot build RangeJoinMatch, some of required attributes are not set " + attributes;
    }
  }
}
