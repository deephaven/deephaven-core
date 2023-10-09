package io.deephaven.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AsOfJoinMatch}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAsOfJoinMatch.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableAsOfJoinMatch.of()}.
 */
@Generated(from = "AsOfJoinMatch", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAsOfJoinMatch extends AsOfJoinMatch {
  private final ColumnName leftColumn;
  private final AsOfJoinRule joinRule;
  private final ColumnName rightColumn;

  private ImmutableAsOfJoinMatch(
      ColumnName leftColumn,
      AsOfJoinRule joinRule,
      ColumnName rightColumn) {
    this.leftColumn = Objects.requireNonNull(leftColumn, "leftColumn");
    this.joinRule = Objects.requireNonNull(joinRule, "joinRule");
    this.rightColumn = Objects.requireNonNull(rightColumn, "rightColumn");
  }

  private ImmutableAsOfJoinMatch(
      ImmutableAsOfJoinMatch original,
      ColumnName leftColumn,
      AsOfJoinRule joinRule,
      ColumnName rightColumn) {
    this.leftColumn = leftColumn;
    this.joinRule = joinRule;
    this.rightColumn = rightColumn;
  }

  /**
   * @return The value of the {@code leftColumn} attribute
   */
  @Override
  public ColumnName leftColumn() {
    return leftColumn;
  }

  /**
   * @return The value of the {@code joinRule} attribute
   */
  @Override
  public AsOfJoinRule joinRule() {
    return joinRule;
  }

  /**
   * @return The value of the {@code rightColumn} attribute
   */
  @Override
  public ColumnName rightColumn() {
    return rightColumn;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinMatch#leftColumn() leftColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for leftColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinMatch withLeftColumn(ColumnName value) {
    if (this.leftColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "leftColumn");
    return new ImmutableAsOfJoinMatch(this, newValue, this.joinRule, this.rightColumn);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinMatch#joinRule() joinRule} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for joinRule
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinMatch withJoinRule(AsOfJoinRule value) {
    AsOfJoinRule newValue = Objects.requireNonNull(value, "joinRule");
    if (this.joinRule == newValue) return this;
    return new ImmutableAsOfJoinMatch(this, this.leftColumn, newValue, this.rightColumn);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinMatch#rightColumn() rightColumn} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rightColumn
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinMatch withRightColumn(ColumnName value) {
    if (this.rightColumn == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "rightColumn");
    return new ImmutableAsOfJoinMatch(this, this.leftColumn, this.joinRule, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAsOfJoinMatch} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAsOfJoinMatch
        && equalTo(0, (ImmutableAsOfJoinMatch) another);
  }

  private boolean equalTo(int synthetic, ImmutableAsOfJoinMatch another) {
    return leftColumn.equals(another.leftColumn)
        && joinRule.equals(another.joinRule)
        && rightColumn.equals(another.rightColumn);
  }

  /**
   * Computes a hash code from attributes: {@code leftColumn}, {@code joinRule}, {@code rightColumn}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + leftColumn.hashCode();
    h += (h << 5) + joinRule.hashCode();
    h += (h << 5) + rightColumn.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AsOfJoinMatch} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AsOfJoinMatch{"
        + "leftColumn=" + leftColumn
        + ", joinRule=" + joinRule
        + ", rightColumn=" + rightColumn
        + "}";
  }

  /**
   * Construct a new immutable {@code AsOfJoinMatch} instance.
   * @param leftColumn The value for the {@code leftColumn} attribute
   * @param joinRule The value for the {@code joinRule} attribute
   * @param rightColumn The value for the {@code rightColumn} attribute
   * @return An immutable AsOfJoinMatch instance
   */
  public static ImmutableAsOfJoinMatch of(ColumnName leftColumn, AsOfJoinRule joinRule, ColumnName rightColumn) {
    return new ImmutableAsOfJoinMatch(leftColumn, joinRule, rightColumn);
  }

  /**
   * Creates an immutable copy of a {@link AsOfJoinMatch} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AsOfJoinMatch instance
   */
  public static ImmutableAsOfJoinMatch copyOf(AsOfJoinMatch instance) {
    if (instance instanceof ImmutableAsOfJoinMatch) {
      return (ImmutableAsOfJoinMatch) instance;
    }
    return ImmutableAsOfJoinMatch.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAsOfJoinMatch ImmutableAsOfJoinMatch}.
   * <pre>
   * ImmutableAsOfJoinMatch.builder()
   *    .leftColumn(io.deephaven.api.ColumnName) // required {@link AsOfJoinMatch#leftColumn() leftColumn}
   *    .joinRule(io.deephaven.api.AsOfJoinRule) // required {@link AsOfJoinMatch#joinRule() joinRule}
   *    .rightColumn(io.deephaven.api.ColumnName) // required {@link AsOfJoinMatch#rightColumn() rightColumn}
   *    .build();
   * </pre>
   * @return A new ImmutableAsOfJoinMatch builder
   */
  public static ImmutableAsOfJoinMatch.Builder builder() {
    return new ImmutableAsOfJoinMatch.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAsOfJoinMatch ImmutableAsOfJoinMatch}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AsOfJoinMatch", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_LEFT_COLUMN = 0x1L;
    private static final long INIT_BIT_JOIN_RULE = 0x2L;
    private static final long INIT_BIT_RIGHT_COLUMN = 0x4L;
    private long initBits = 0x7L;

    private @Nullable ColumnName leftColumn;
    private @Nullable AsOfJoinRule joinRule;
    private @Nullable ColumnName rightColumn;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AsOfJoinMatch} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AsOfJoinMatch instance) {
      Objects.requireNonNull(instance, "instance");
      leftColumn(instance.leftColumn());
      joinRule(instance.joinRule());
      rightColumn(instance.rightColumn());
      return this;
    }

    /**
     * Initializes the value for the {@link AsOfJoinMatch#leftColumn() leftColumn} attribute.
     * @param leftColumn The value for leftColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder leftColumn(ColumnName leftColumn) {
      this.leftColumn = Objects.requireNonNull(leftColumn, "leftColumn");
      initBits &= ~INIT_BIT_LEFT_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link AsOfJoinMatch#joinRule() joinRule} attribute.
     * @param joinRule The value for joinRule 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder joinRule(AsOfJoinRule joinRule) {
      this.joinRule = Objects.requireNonNull(joinRule, "joinRule");
      initBits &= ~INIT_BIT_JOIN_RULE;
      return this;
    }

    /**
     * Initializes the value for the {@link AsOfJoinMatch#rightColumn() rightColumn} attribute.
     * @param rightColumn The value for rightColumn 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rightColumn(ColumnName rightColumn) {
      this.rightColumn = Objects.requireNonNull(rightColumn, "rightColumn");
      initBits &= ~INIT_BIT_RIGHT_COLUMN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAsOfJoinMatch ImmutableAsOfJoinMatch}.
     * @return An immutable instance of AsOfJoinMatch
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAsOfJoinMatch build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAsOfJoinMatch(null, leftColumn, joinRule, rightColumn);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT_COLUMN) != 0) attributes.add("leftColumn");
      if ((initBits & INIT_BIT_JOIN_RULE) != 0) attributes.add("joinRule");
      if ((initBits & INIT_BIT_RIGHT_COLUMN) != 0) attributes.add("rightColumn");
      return "Cannot build AsOfJoinMatch, some of required attributes are not set " + attributes;
    }
  }
}
