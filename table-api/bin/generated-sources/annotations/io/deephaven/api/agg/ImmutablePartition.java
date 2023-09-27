package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Partition}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePartition.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutablePartition.of()}.
 */
@Generated(from = "Partition", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutablePartition extends Partition {
  private final ColumnName column;
  private final boolean includeGroupByColumns;

  private ImmutablePartition(ColumnName column) {
    this.column = Objects.requireNonNull(column, "column");
    this.includeGroupByColumns = super.includeGroupByColumns();
  }

  private ImmutablePartition(ImmutablePartition.Builder builder) {
    this.column = builder.column;
    this.includeGroupByColumns = builder.includeGroupByColumnsIsSet()
        ? builder.includeGroupByColumns
        : super.includeGroupByColumns();
  }

  private ImmutablePartition(ColumnName column, boolean includeGroupByColumns) {
    this.column = column;
    this.includeGroupByColumns = includeGroupByColumns;
  }

  /**
   * @return The value of the {@code column} attribute
   */
  @Override
  public ColumnName column() {
    return column;
  }

  /**
   * Whether group-by columns (sometimes referred to as "key" columns) should be included in the output sub-tables.
   * @return Whether to include group-by columns in the output sub-tables
   */
  @Override
  public boolean includeGroupByColumns() {
    return includeGroupByColumns;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Partition#column() column} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for column
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePartition withColumn(ColumnName value) {
    if (this.column == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "column");
    return new ImmutablePartition(newValue, this.includeGroupByColumns);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link Partition#includeGroupByColumns() includeGroupByColumns} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for includeGroupByColumns
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePartition withIncludeGroupByColumns(boolean value) {
    if (this.includeGroupByColumns == value) return this;
    return new ImmutablePartition(this.column, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePartition} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePartition
        && equalTo(0, (ImmutablePartition) another);
  }

  private boolean equalTo(int synthetic, ImmutablePartition another) {
    return column.equals(another.column)
        && includeGroupByColumns == another.includeGroupByColumns;
  }

  /**
   * Computes a hash code from attributes: {@code column}, {@code includeGroupByColumns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + column.hashCode();
    h += (h << 5) + Boolean.hashCode(includeGroupByColumns);
    return h;
  }

  /**
   * Prints the immutable value {@code Partition} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Partition{"
        + "column=" + column
        + ", includeGroupByColumns=" + includeGroupByColumns
        + "}";
  }

  /**
   * Construct a new immutable {@code Partition} instance.
   * @param column The value for the {@code column} attribute
   * @return An immutable Partition instance
   */
  public static ImmutablePartition of(ColumnName column) {
    return new ImmutablePartition(column);
  }

  /**
   * Creates an immutable copy of a {@link Partition} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Partition instance
   */
  public static ImmutablePartition copyOf(Partition instance) {
    if (instance instanceof ImmutablePartition) {
      return (ImmutablePartition) instance;
    }
    return ImmutablePartition.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutablePartition ImmutablePartition}.
   * <pre>
   * ImmutablePartition.builder()
   *    .column(io.deephaven.api.ColumnName) // required {@link Partition#column() column}
   *    .includeGroupByColumns(boolean) // optional {@link Partition#includeGroupByColumns() includeGroupByColumns}
   *    .build();
   * </pre>
   * @return A new ImmutablePartition builder
   */
  public static ImmutablePartition.Builder builder() {
    return new ImmutablePartition.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePartition ImmutablePartition}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Partition", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN = 0x1L;
    private static final long OPT_BIT_INCLUDE_GROUP_BY_COLUMNS = 0x1L;
    private long initBits = 0x1L;
    private long optBits;

    private @Nullable ColumnName column;
    private boolean includeGroupByColumns;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Partition} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Partition instance) {
      Objects.requireNonNull(instance, "instance");
      column(instance.column());
      includeGroupByColumns(instance.includeGroupByColumns());
      return this;
    }

    /**
     * Initializes the value for the {@link Partition#column() column} attribute.
     * @param column The value for column 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder column(ColumnName column) {
      this.column = Objects.requireNonNull(column, "column");
      initBits &= ~INIT_BIT_COLUMN;
      return this;
    }

    /**
     * Initializes the value for the {@link Partition#includeGroupByColumns() includeGroupByColumns} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link Partition#includeGroupByColumns() includeGroupByColumns}.</em>
     * @param includeGroupByColumns The value for includeGroupByColumns 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder includeGroupByColumns(boolean includeGroupByColumns) {
      this.includeGroupByColumns = includeGroupByColumns;
      optBits |= OPT_BIT_INCLUDE_GROUP_BY_COLUMNS;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePartition ImmutablePartition}.
     * @return An immutable instance of Partition
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePartition build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutablePartition(this);
    }

    private boolean includeGroupByColumnsIsSet() {
      return (optBits & OPT_BIT_INCLUDE_GROUP_BY_COLUMNS) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN) != 0) attributes.add("column");
      return "Cannot build Partition, some of required attributes are not set " + attributes;
    }
  }
}
