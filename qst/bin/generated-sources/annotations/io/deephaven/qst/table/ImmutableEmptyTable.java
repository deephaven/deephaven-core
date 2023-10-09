package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.List;
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
final class ImmutableEmptyTable extends EmptyTable {
  private transient final int depth;
  private final long size;

  private ImmutableEmptyTable(long size) {
    this.size = size;
    this.depth = super.depth();
  }

  private ImmutableEmptyTable(ImmutableEmptyTable.Builder builder) {
    this.size = builder.size;
    this.depth = super.depth();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
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
    h += (h << 5) + getClass().hashCode();
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
     * Initializes the value for the {@link EmptyTable#size() size} attribute.
     * @param size The value for size 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder size(long size) {
      checkNotIsSet(sizeIsSet(), "size");
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
      checkRequiredAttributes();
      return ImmutableEmptyTable.validate(new ImmutableEmptyTable(this));
    }

    private boolean sizeIsSet() {
      return (initBits & INIT_BIT_SIZE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of EmptyTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!sizeIsSet()) attributes.add("size");
      return "Cannot build EmptyTable, some of required attributes are not set " + attributes;
    }
  }
}
