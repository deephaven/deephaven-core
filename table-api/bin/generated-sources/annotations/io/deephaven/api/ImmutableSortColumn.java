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
 * Immutable implementation of {@link SortColumn}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSortColumn.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSortColumn.of()}.
 */
@Generated(from = "SortColumn", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableSortColumn extends SortColumn {
  private final ColumnName column;
  private final SortColumn.Order order;

  private ImmutableSortColumn(ColumnName column, SortColumn.Order order) {
    this.column = Objects.requireNonNull(column, "column");
    this.order = Objects.requireNonNull(order, "order");
  }

  private ImmutableSortColumn(
      ImmutableSortColumn original,
      ColumnName column,
      SortColumn.Order order) {
    this.column = column;
    this.order = order;
  }

  /**
   * The column name.
   * @return the column name
   */
  @Override
  public ColumnName column() {
    return column;
  }

  /**
   * The order.
   * @return the order
   */
  @Override
  public SortColumn.Order order() {
    return order;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SortColumn#column() column} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for column
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSortColumn withColumn(ColumnName value) {
    if (this.column == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "column");
    return new ImmutableSortColumn(this, newValue, this.order);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SortColumn#order() order} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for order
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSortColumn withOrder(SortColumn.Order value) {
    SortColumn.Order newValue = Objects.requireNonNull(value, "order");
    if (this.order == newValue) return this;
    return new ImmutableSortColumn(this, this.column, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSortColumn} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSortColumn
        && equalTo(0, (ImmutableSortColumn) another);
  }

  private boolean equalTo(int synthetic, ImmutableSortColumn another) {
    return column.equals(another.column)
        && order.equals(another.order);
  }

  /**
   * Computes a hash code from attributes: {@code column}, {@code order}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + column.hashCode();
    h += (h << 5) + order.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SortColumn} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SortColumn{"
        + "column=" + column
        + ", order=" + order
        + "}";
  }

  /**
   * Construct a new immutable {@code SortColumn} instance.
   * @param column The value for the {@code column} attribute
   * @param order The value for the {@code order} attribute
   * @return An immutable SortColumn instance
   */
  public static ImmutableSortColumn of(ColumnName column, SortColumn.Order order) {
    return new ImmutableSortColumn(column, order);
  }

  /**
   * Creates an immutable copy of a {@link SortColumn} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SortColumn instance
   */
  public static ImmutableSortColumn copyOf(SortColumn instance) {
    if (instance instanceof ImmutableSortColumn) {
      return (ImmutableSortColumn) instance;
    }
    return ImmutableSortColumn.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSortColumn ImmutableSortColumn}.
   * <pre>
   * ImmutableSortColumn.builder()
   *    .column(io.deephaven.api.ColumnName) // required {@link SortColumn#column() column}
   *    .order(io.deephaven.api.SortColumn.Order) // required {@link SortColumn#order() order}
   *    .build();
   * </pre>
   * @return A new ImmutableSortColumn builder
   */
  public static ImmutableSortColumn.Builder builder() {
    return new ImmutableSortColumn.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSortColumn ImmutableSortColumn}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SortColumn", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN = 0x1L;
    private static final long INIT_BIT_ORDER = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ColumnName column;
    private @Nullable SortColumn.Order order;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SortColumn} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SortColumn instance) {
      Objects.requireNonNull(instance, "instance");
      column(instance.column());
      order(instance.order());
      return this;
    }

    /**
     * Initializes the value for the {@link SortColumn#column() column} attribute.
     * @param column The value for column 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder column(ColumnName column) {
      this.column = Objects.requireNonNull(column, "column");
      initBits &= ~INIT_BIT_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link SortColumn#order() order} attribute.
     * @param order The value for order 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder order(SortColumn.Order order) {
      this.order = Objects.requireNonNull(order, "order");
      initBits &= ~INIT_BIT_ORDER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSortColumn ImmutableSortColumn}.
     * @return An immutable instance of SortColumn
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSortColumn build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSortColumn(null, column, order);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN) != 0) attributes.add("column");
      if ((initBits & INIT_BIT_ORDER) != 0) attributes.add("order");
      return "Cannot build SortColumn, some of required attributes are not set " + attributes;
    }
  }
}
