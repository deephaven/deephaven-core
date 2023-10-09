package io.deephaven.qst.table;

import io.deephaven.api.filter.Filter;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link WhereTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableWhereTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableWhereTable.of()}.
 */
@Generated(from = "WhereTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableWhereTable extends WhereTable {
  private transient final int depth;
  private final TableSpec parent;
  private final Filter filter;
  private transient final int hashCode;

  private ImmutableWhereTable(TableSpec parent, Filter filter) {
    this.parent = Objects.requireNonNull(parent, "parent");
    this.filter = Objects.requireNonNull(filter, "filter");
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableWhereTable(
      ImmutableWhereTable original,
      TableSpec parent,
      Filter filter) {
    this.parent = parent;
    this.filter = filter;
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
   * @return The value of the {@code filter} attribute
   */
  @Override
  public Filter filter() {
    return filter;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WhereTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWhereTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableWhereTable(this, newValue, this.filter));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WhereTable#filter() filter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for filter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWhereTable withFilter(Filter value) {
    if (this.filter == value) return this;
    Filter newValue = Objects.requireNonNull(value, "filter");
    return validate(new ImmutableWhereTable(this, this.parent, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableWhereTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableWhereTable
        && equalTo(0, (ImmutableWhereTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableWhereTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && filter.equals(another.filter);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code filter}.
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
    h += (h << 5) + filter.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableWhereTable, WeakReference<ImmutableWhereTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code WhereTable} instance.
   * @param parent The value for the {@code parent} attribute
   * @param filter The value for the {@code filter} attribute
   * @return An immutable WhereTable instance
   */
  public static ImmutableWhereTable of(TableSpec parent, Filter filter) {
    return validate(new ImmutableWhereTable(parent, filter));
  }

  private static ImmutableWhereTable validate(ImmutableWhereTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableWhereTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableWhereTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link WhereTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable WhereTable instance
   */
  public static ImmutableWhereTable copyOf(WhereTable instance) {
    if (instance instanceof ImmutableWhereTable) {
      return (ImmutableWhereTable) instance;
    }
    return ImmutableWhereTable.builder()
        .parent(instance.parent())
        .filter(instance.filter())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableWhereTable ImmutableWhereTable}.
   * <pre>
   * ImmutableWhereTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link WhereTable#parent() parent}
   *    .filter(io.deephaven.api.filter.Filter) // required {@link WhereTable#filter() filter}
   *    .build();
   * </pre>
   * @return A new ImmutableWhereTable builder
   */
  public static ImmutableWhereTable.Builder builder() {
    return new ImmutableWhereTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableWhereTable ImmutableWhereTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "WhereTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long INIT_BIT_FILTER = 0x2L;
    private long initBits = 0x3L;

    private TableSpec parent;
    private Filter filter;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link WhereTable#parent() parent} attribute.
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
     * Initializes the value for the {@link WhereTable#filter() filter} attribute.
     * @param filter The value for filter 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder filter(Filter filter) {
      checkNotIsSet(filterIsSet(), "filter");
      this.filter = Objects.requireNonNull(filter, "filter");
      initBits &= ~INIT_BIT_FILTER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableWhereTable ImmutableWhereTable}.
     * @return An immutable instance of WhereTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableWhereTable build() {
      checkRequiredAttributes();
      return ImmutableWhereTable.validate(new ImmutableWhereTable(null, parent, filter));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private boolean filterIsSet() {
      return (initBits & INIT_BIT_FILTER) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of WhereTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
      if (!filterIsSet()) attributes.add("filter");
      return "Cannot build WhereTable, some of required attributes are not set " + attributes;
    }
  }
}
