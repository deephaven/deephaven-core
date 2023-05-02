package io.deephaven.qst.table;

import java.io.ObjectStreamException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
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
final class ImmutableSnapshotTable extends SnapshotTable {
  private final int depth;
  private final TableSpec base;
  private final int hashCode;

  private ImmutableSnapshotTable(TableSpec base) {
    this.base = Objects.requireNonNull(base, "base");
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableSnapshotTable(ImmutableSnapshotTable original, TableSpec base) {
    this.base = base;
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
    return validate(new ImmutableSnapshotTable(this, newValue));
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
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && base.equals(another.base);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code base}.
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
    h += (h << 5) + base.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableSnapshotTable, WeakReference<ImmutableSnapshotTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code SnapshotTable} instance.
   * @param base The value for the {@code base} attribute
   * @return An immutable SnapshotTable instance
   */
  public static ImmutableSnapshotTable of(TableSpec base) {
    return validate(new ImmutableSnapshotTable(base));
  }

  private static ImmutableSnapshotTable validate(ImmutableSnapshotTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableSnapshotTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableSnapshotTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
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
        .base(instance.base())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableSnapshotTable(this, this.base));
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
     * Initializes the value for the {@link SnapshotTable#base() base} attribute.
     * @param base The value for base 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder base(TableSpec base) {
      checkNotIsSet(baseIsSet(), "base");
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
      checkRequiredAttributes();
      return ImmutableSnapshotTable.validate(new ImmutableSnapshotTable(null, base));
    }

    private boolean baseIsSet() {
      return (initBits & INIT_BIT_BASE) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SnapshotTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!baseIsSet()) attributes.add("base");
      return "Cannot build SnapshotTable, some of required attributes are not set " + attributes;
    }
  }
}
