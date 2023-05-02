package io.deephaven.qst.table;

import io.deephaven.api.filter.Filter;
import java.io.ObjectStreamException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
 */
@Generated(from = "WhereTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableWhereTable extends WhereTable {
  private final int depth;
  private final TableSpec parent;
  private final List<Filter> filters;
  private final int hashCode;

  private ImmutableWhereTable(TableSpec parent, List<Filter> filters) {
    this.parent = parent;
    this.filters = filters;
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
   * @return The value of the {@code filters} attribute
   */
  @Override
  public List<Filter> filters() {
    return filters;
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
    return validate(new ImmutableWhereTable(newValue, this.filters));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link WhereTable#filters() filters}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableWhereTable withFilters(Filter... elements) {
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableWhereTable(this.parent, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link WhereTable#filters() filters}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of filters elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableWhereTable withFilters(Iterable<? extends Filter> elements) {
    if (this.filters == elements) return this;
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableWhereTable(this.parent, newValue));
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
        && filters.equals(another.filters);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code filters}.
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
    h += (h << 5) + filters.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableWhereTable, WeakReference<ImmutableWhereTable>> INTERNER =
        new WeakHashMap<>();
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
        .addAllFilters(instance.filters())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableWhereTable(this.parent, this.filters));
  }

  /**
   * Creates a builder for {@link ImmutableWhereTable ImmutableWhereTable}.
   * <pre>
   * ImmutableWhereTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link WhereTable#parent() parent}
   *    .addFilters|addAllFilters(io.deephaven.api.filter.Filter) // {@link WhereTable#filters() filters} elements
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
  public static final class Builder implements WhereTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private long initBits = 0x1L;

    private TableSpec parent;
    private final List<Filter> filters = new ArrayList<Filter>();

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
     * Adds one element to {@link WhereTable#filters() filters} list.
     * @param element A filters element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFilters(Filter element) {
      this.filters.add(Objects.requireNonNull(element, "filters element"));
      return this;
    }

    /**
     * Adds elements to {@link WhereTable#filters() filters} list.
     * @param elements An array of filters elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFilters(Filter... elements) {
      for (Filter element : elements) {
        this.filters.add(Objects.requireNonNull(element, "filters element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link WhereTable#filters() filters} list.
     * @param elements An iterable of filters elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllFilters(Iterable<? extends Filter> elements) {
      for (Filter element : elements) {
        this.filters.add(Objects.requireNonNull(element, "filters element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableWhereTable ImmutableWhereTable}.
     * @return An immutable instance of WhereTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableWhereTable build() {
      checkRequiredAttributes();
      return ImmutableWhereTable.validate(new ImmutableWhereTable(parent, createUnmodifiableList(true, filters)));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
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
      return "Cannot build WhereTable, some of required attributes are not set " + attributes;
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
