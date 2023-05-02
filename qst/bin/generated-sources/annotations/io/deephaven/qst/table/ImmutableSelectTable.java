package io.deephaven.qst.table;

import io.deephaven.api.Selectable;
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
 * Immutable implementation of {@link SelectTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSelectTable.builder()}.
 */
@Generated(from = "SelectTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableSelectTable extends SelectTable {
  private final int depth;
  private final TableSpec parent;
  private final List<Selectable> columns;
  private final int hashCode;

  private ImmutableSelectTable(TableSpec parent, List<Selectable> columns) {
    this.parent = parent;
    this.columns = columns;
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
   * @return The value of the {@code columns} attribute
   */
  @Override
  public List<Selectable> columns() {
    return columns;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SelectTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSelectTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableSelectTable(newValue, this.columns));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SelectTable#columns() columns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSelectTable withColumns(Selectable... elements) {
    List<Selectable> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableSelectTable(this.parent, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SelectTable#columns() columns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSelectTable withColumns(Iterable<? extends Selectable> elements) {
    if (this.columns == elements) return this;
    List<Selectable> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableSelectTable(this.parent, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSelectTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSelectTable
        && equalTo(0, (ImmutableSelectTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableSelectTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && columns.equals(another.columns);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code columns}.
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
    h += (h << 5) + columns.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableSelectTable, WeakReference<ImmutableSelectTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableSelectTable validate(ImmutableSelectTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableSelectTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableSelectTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link SelectTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SelectTable instance
   */
  public static ImmutableSelectTable copyOf(SelectTable instance) {
    if (instance instanceof ImmutableSelectTable) {
      return (ImmutableSelectTable) instance;
    }
    return ImmutableSelectTable.builder()
        .parent(instance.parent())
        .addAllColumns(instance.columns())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableSelectTable(this.parent, this.columns));
  }

  /**
   * Creates a builder for {@link ImmutableSelectTable ImmutableSelectTable}.
   * <pre>
   * ImmutableSelectTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link SelectTable#parent() parent}
   *    .addColumns|addAllColumns(io.deephaven.api.Selectable) // {@link SelectTable#columns() columns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableSelectTable builder
   */
  public static ImmutableSelectTable.Builder builder() {
    return new ImmutableSelectTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSelectTable ImmutableSelectTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SelectTable", generator = "Immutables")
  public static final class Builder implements SelectTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private long initBits = 0x1L;

    private TableSpec parent;
    private final List<Selectable> columns = new ArrayList<Selectable>();

    private Builder() {
    }

    /**
     * Initializes the value for the {@link SelectTable#parent() parent} attribute.
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
     * Adds one element to {@link SelectTable#columns() columns} list.
     * @param element A columns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(Selectable element) {
      this.columns.add(Objects.requireNonNull(element, "columns element"));
      return this;
    }

    /**
     * Adds elements to {@link SelectTable#columns() columns} list.
     * @param elements An array of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(Selectable... elements) {
      for (Selectable element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link SelectTable#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllColumns(Iterable<? extends Selectable> elements) {
      for (Selectable element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableSelectTable ImmutableSelectTable}.
     * @return An immutable instance of SelectTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSelectTable build() {
      checkRequiredAttributes();
      return ImmutableSelectTable.validate(new ImmutableSelectTable(parent, createUnmodifiableList(true, columns)));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SelectTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
      return "Cannot build SelectTable, some of required attributes are not set " + attributes;
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
