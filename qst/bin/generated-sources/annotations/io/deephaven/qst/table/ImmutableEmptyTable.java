package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link EmptyTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableEmptyTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableEmptyTable.of()}.
 */
@Generated(from = "EmptyTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableEmptyTable extends EmptyTable {
  private transient final int depth;
  private final long size;

  private ImmutableEmptyTable(long size) {
    this.size = size;
    this.depth = super.depth();
  }

  /**
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code size} attribute
   */
  @Override
  public long size() {
    return size;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link EmptyTable#size() size} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for size
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableEmptyTable withSize(long value) {
    if (this.size == value) return this;
    return validate(new ImmutableEmptyTable(value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableEmptyTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableEmptyTable
        && equalTo(0, (ImmutableEmptyTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableEmptyTable another) {
    return depth == another.depth
        && size == another.size;
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code size}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + Long.hashCode(size);
    return h;
  }

  /**
   * Construct a new immutable {@code EmptyTable} instance.
   * @param size The value for the {@code size} attribute
   * @return An immutable EmptyTable instance
   */
  public static ImmutableEmptyTable of(long size) {
    return validate(new ImmutableEmptyTable(size));
  }

  private static ImmutableEmptyTable validate(ImmutableEmptyTable instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link EmptyTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable EmptyTable instance
   */
  public static ImmutableEmptyTable copyOf(EmptyTable instance) {
    if (instance instanceof ImmutableEmptyTable) {
      return (ImmutableEmptyTable) instance;
    }
    return ImmutableEmptyTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableEmptyTable ImmutableEmptyTable}.
   * <pre>
   * ImmutableEmptyTable.builder()
   *    .size(long) // required {@link EmptyTable#size() size}
   *    .build();
   * </pre>
   * @return A new ImmutableEmptyTable builder
   */
  public static ImmutableEmptyTable.Builder builder() {
    return new ImmutableEmptyTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableEmptyTable ImmutableEmptyTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "EmptyTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_SIZE = 0x1L;
    private long initBits = 0x1L;

    private long size;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code EmptyTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(EmptyTable instance) {
      Objects.requireNonNull(instance, "instance");
      size(instance.size());
      return this;
    }

    /**
     * Initializes the value for the {@link EmptyTable#size() size} attribute.
     * @param size The value for size 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder size(long size) {
      this.size = size;
      initBits &= ~INIT_BIT_SIZE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableEmptyTable ImmutableEmptyTable}.
     * @return An immutable instance of EmptyTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableEmptyTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableEmptyTable.validate(new ImmutableEmptyTable(size));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SIZE) != 0) attributes.add("size");
      return "Cannot build EmptyTable, some of required attributes are not set " + attributes;
    }
  }
}
