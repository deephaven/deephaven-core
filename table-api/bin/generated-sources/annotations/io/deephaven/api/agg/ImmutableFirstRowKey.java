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
 * Immutable implementation of {@link FirstRowKey}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFirstRowKey.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFirstRowKey.of()}.
 */
@Generated(from = "FirstRowKey", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFirstRowKey extends FirstRowKey {
  private final ColumnName column;

  private ImmutableFirstRowKey(ColumnName column) {
    this.column = Objects.requireNonNull(column, "column");
  }

  private ImmutableFirstRowKey(ImmutableFirstRowKey original, ColumnName column) {
    this.column = column;
  }

  /**
   * @return The value of the {@code column} attribute
   */
  @Override
  public ColumnName column() {
    return column;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FirstRowKey#column() column} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for column
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFirstRowKey withColumn(ColumnName value) {
    if (this.column == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "column");
    return new ImmutableFirstRowKey(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFirstRowKey} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFirstRowKey
        && equalTo(0, (ImmutableFirstRowKey) another);
  }

  private boolean equalTo(int synthetic, ImmutableFirstRowKey another) {
    return column.equals(another.column);
  }

  /**
   * Computes a hash code from attributes: {@code column}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + column.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FirstRowKey} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FirstRowKey{"
        + "column=" + column
        + "}";
  }

  /**
   * Construct a new immutable {@code FirstRowKey} instance.
   * @param column The value for the {@code column} attribute
   * @return An immutable FirstRowKey instance
   */
  public static ImmutableFirstRowKey of(ColumnName column) {
    return new ImmutableFirstRowKey(column);
  }

  /**
   * Creates an immutable copy of a {@link FirstRowKey} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FirstRowKey instance
   */
  public static ImmutableFirstRowKey copyOf(FirstRowKey instance) {
    if (instance instanceof ImmutableFirstRowKey) {
      return (ImmutableFirstRowKey) instance;
    }
    return ImmutableFirstRowKey.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFirstRowKey ImmutableFirstRowKey}.
   * <pre>
   * ImmutableFirstRowKey.builder()
   *    .column(io.deephaven.api.ColumnName) // required {@link FirstRowKey#column() column}
   *    .build();
   * </pre>
   * @return A new ImmutableFirstRowKey builder
   */
  public static ImmutableFirstRowKey.Builder builder() {
    return new ImmutableFirstRowKey.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFirstRowKey ImmutableFirstRowKey}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FirstRowKey", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_COLUMN = 0x1L;
    private long initBits = 0x1L;

    private @Nullable ColumnName column;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FirstRowKey} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FirstRowKey instance) {
      Objects.requireNonNull(instance, "instance");
      column(instance.column());
      return this;
    }

    /**
     * Initializes the value for the {@link FirstRowKey#column() column} attribute.
     * @param column The value for column 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder column(ColumnName column) {
      this.column = Objects.requireNonNull(column, "column");
      initBits &= ~INIT_BIT_COLUMN;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFirstRowKey ImmutableFirstRowKey}.
     * @return An immutable instance of FirstRowKey
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFirstRowKey build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFirstRowKey(null, column);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_COLUMN) != 0) attributes.add("column");
      return "Cannot build FirstRowKey, some of required attributes are not set " + attributes;
    }
  }
}
