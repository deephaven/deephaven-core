package io.deephaven.qst.table;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link HeadTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableHeadTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableHeadTable.of()}.
 */
@Generated(from = "HeadTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableHeadTable extends HeadTable {
  private transient final int depth;
  private final TableSpec parent;
  private final long size;
  private transient final int hashCode;

  private ImmutableHeadTable(TableSpec parent, long size) {
    this.parent = Objects.requireNonNull(parent, "parent");
    this.size = size;
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableHeadTable(ImmutableHeadTable original, TableSpec parent, long size) {
    this.parent = parent;
    this.size = size;
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code parent} attribute
   */
  @Override
  public TableSpec parent() {
    return parent;
  }

  /**
   * @return The value of the {@code size} attribute
   */
  @Override
  public long size() {
    return size;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link HeadTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableHeadTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableHeadTable(this, newValue, this.size));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link HeadTable#size() size} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for size
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableHeadTable withSize(long value) {
    if (this.size == value) return this;
    return validate(new ImmutableHeadTable(this, this.parent, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableHeadTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableHeadTable
        && equalTo(0, (ImmutableHeadTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableHeadTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && size == another.size;
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code size}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + parent.hashCode();
    h += (h << 5) + Long.hashCode(size);
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableHeadTable, WeakReference<ImmutableHeadTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code HeadTable} instance.
   * @param parent The value for the {@code parent} attribute
   * @param size The value for the {@code size} attribute
   * @return An immutable HeadTable instance
   */
  public static ImmutableHeadTable of(TableSpec parent, long size) {
    return validate(new ImmutableHeadTable(parent, size));
  }

  private static ImmutableHeadTable validate(ImmutableHeadTable instance) {
    instance.checkSize();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableHeadTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableHeadTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link HeadTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable HeadTable instance
   */
  public static ImmutableHeadTable copyOf(HeadTable instance) {
    if (instance instanceof ImmutableHeadTable) {
      return (ImmutableHeadTable) instance;
    }
    return ImmutableHeadTable.builder()
        .parent(instance.parent())
        .size(instance.size())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableHeadTable ImmutableHeadTable}.
   * <pre>
   * ImmutableHeadTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link HeadTable#parent() parent}
   *    .size(long) // required {@link HeadTable#size() size}
   *    .build();
   * </pre>
   * @return A new ImmutableHeadTable builder
   */
  public static ImmutableHeadTable.Builder builder() {
    return new ImmutableHeadTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableHeadTable ImmutableHeadTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "HeadTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long INIT_BIT_SIZE = 0x2L;
    private long initBits = 0x3L;

    private TableSpec parent;
    private long size;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link HeadTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
      checkNotIsSet(parentIsSet(), "parent");
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
      return this;
    }

    /**
     * Initializes the value for the {@link HeadTable#size() size} attribute.
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
     * Builds a new {@link ImmutableHeadTable ImmutableHeadTable}.
     * @return An immutable instance of HeadTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableHeadTable build() {
      checkRequiredAttributes();
      return ImmutableHeadTable.validate(new ImmutableHeadTable(null, parent, size));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private boolean sizeIsSet() {
      return (initBits & INIT_BIT_SIZE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of HeadTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
      if (!sizeIsSet()) attributes.add("size");
      return "Cannot build HeadTable, some of required attributes are not set " + attributes;
    }
  }
}
