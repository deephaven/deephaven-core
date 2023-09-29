package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SnapshotTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSnapshotTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableSnapshotTable.of()}.
 */
@Generated(from = "SnapshotTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableSnapshotTable extends SnapshotTable {
  private transient final int depth;
  private final TableSpec base;

  private ImmutableSnapshotTable(TableSpec base) {
    this.base = Objects.requireNonNull(base, "base");
    this.depth = super.depth();
  }

  private ImmutableSnapshotTable(ImmutableSnapshotTable original, TableSpec base) {
    this.base = base;
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
   * @return The value of the {@code base} attribute
   */
  @Override
  public TableSpec base() {
    return base;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SnapshotTable#base() base} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for base
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSnapshotTable withBase(TableSpec value) {
    if (this.base == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "base");
    return new ImmutableSnapshotTable(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSnapshotTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSnapshotTable
        && equalTo(0, (ImmutableSnapshotTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableSnapshotTable another) {
    return depth == another.depth
        && base.equals(another.base);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code base}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + base.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code SnapshotTable} instance.
   * @param base The value for the {@code base} attribute
   * @return An immutable SnapshotTable instance
   */
  public static ImmutableSnapshotTable of(TableSpec base) {
    return new ImmutableSnapshotTable(base);
  }

  /**
   * Creates an immutable copy of a {@link SnapshotTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SnapshotTable instance
   */
  public static ImmutableSnapshotTable copyOf(SnapshotTable instance) {
    if (instance instanceof ImmutableSnapshotTable) {
      return (ImmutableSnapshotTable) instance;
    }
    return ImmutableSnapshotTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSnapshotTable ImmutableSnapshotTable}.
   * <pre>
   * ImmutableSnapshotTable.builder()
   *    .base(io.deephaven.qst.table.TableSpec) // required {@link SnapshotTable#base() base}
   *    .build();
   * </pre>
   * @return A new ImmutableSnapshotTable builder
   */
  public static ImmutableSnapshotTable.Builder builder() {
    return new ImmutableSnapshotTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSnapshotTable ImmutableSnapshotTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SnapshotTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_BASE = 0x1L;
    private long initBits = 0x1L;

    private TableSpec base;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SnapshotTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SnapshotTable instance) {
      Objects.requireNonNull(instance, "instance");
      base(instance.base());
      return this;
    }

    /**
     * Initializes the value for the {@link SnapshotTable#base() base} attribute.
     * @param base The value for base 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder base(TableSpec base) {
      this.base = Objects.requireNonNull(base, "base");
      initBits &= ~INIT_BIT_BASE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSnapshotTable ImmutableSnapshotTable}.
     * @return An immutable instance of SnapshotTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSnapshotTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSnapshotTable(null, base);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_BASE) != 0) attributes.add("base");
      return "Cannot build SnapshotTable, some of required attributes are not set " + attributes;
    }
  }
}
