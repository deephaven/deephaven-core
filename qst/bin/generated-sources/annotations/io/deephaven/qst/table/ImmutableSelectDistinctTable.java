package io.deephaven.qst.table;

import io.deephaven.api.Selectable;
<<<<<<< HEAD
import java.lang.ref.WeakReference;
=======
>>>>>>> main
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
<<<<<<< HEAD
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
=======
import java.util.Objects;
>>>>>>> main
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SelectDistinctTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSelectDistinctTable.builder()}.
 */
@Generated(from = "SelectDistinctTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableSelectDistinctTable extends SelectDistinctTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<Selectable> columns;
  private transient final int hashCode;
=======
public final class ImmutableSelectDistinctTable extends SelectDistinctTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<Selectable> columns;
>>>>>>> main

  private ImmutableSelectDistinctTable(TableSpec parent, List<Selectable> columns) {
    this.parent = parent;
    this.columns = columns;
    this.depth = super.depth();
<<<<<<< HEAD
    this.hashCode = computeHashCode();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
=======
  }

  /**
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
>>>>>>> main
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
   * Copy the current immutable object by setting a value for the {@link SelectDistinctTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSelectDistinctTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
<<<<<<< HEAD
    return validate(new ImmutableSelectDistinctTable(newValue, this.columns));
=======
    return new ImmutableSelectDistinctTable(newValue, this.columns);
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SelectDistinctTable#columns() columns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSelectDistinctTable withColumns(Selectable... elements) {
    List<Selectable> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
<<<<<<< HEAD
    return validate(new ImmutableSelectDistinctTable(this.parent, newValue));
=======
    return new ImmutableSelectDistinctTable(this.parent, newValue);
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SelectDistinctTable#columns() columns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSelectDistinctTable withColumns(Iterable<? extends Selectable> elements) {
    if (this.columns == elements) return this;
    List<Selectable> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
<<<<<<< HEAD
    return validate(new ImmutableSelectDistinctTable(this.parent, newValue));
=======
    return new ImmutableSelectDistinctTable(this.parent, newValue);
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSelectDistinctTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSelectDistinctTable
        && equalTo(0, (ImmutableSelectDistinctTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableSelectDistinctTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
=======
>>>>>>> main
    return depth == another.depth
        && parent.equals(another.parent)
        && columns.equals(another.columns);
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code columns}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code parent}, {@code columns}.
>>>>>>> main
   * @return hashCode value
   */
  @Override
  public int hashCode() {
<<<<<<< HEAD
    return hashCode;
  }

  private int computeHashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
=======
    int h = 5381;
>>>>>>> main
    h += (h << 5) + depth;
    h += (h << 5) + parent.hashCode();
    h += (h << 5) + columns.hashCode();
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableSelectDistinctTable, WeakReference<ImmutableSelectDistinctTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableSelectDistinctTable validate(ImmutableSelectDistinctTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableSelectDistinctTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableSelectDistinctTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

=======
>>>>>>> main
  /**
   * Creates an immutable copy of a {@link SelectDistinctTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SelectDistinctTable instance
   */
  public static ImmutableSelectDistinctTable copyOf(SelectDistinctTable instance) {
    if (instance instanceof ImmutableSelectDistinctTable) {
      return (ImmutableSelectDistinctTable) instance;
    }
    return ImmutableSelectDistinctTable.builder()
<<<<<<< HEAD
        .parent(instance.parent())
        .addAllColumns(instance.columns())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSelectDistinctTable ImmutableSelectDistinctTable}.
   * <pre>
   * ImmutableSelectDistinctTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link SelectDistinctTable#parent() parent}
   *    .addColumns|addAllColumns(io.deephaven.api.Selectable) // {@link SelectDistinctTable#columns() columns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableSelectDistinctTable builder
   */
  public static ImmutableSelectDistinctTable.Builder builder() {
    return new ImmutableSelectDistinctTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSelectDistinctTable ImmutableSelectDistinctTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SelectDistinctTable", generator = "Immutables")
  public static final class Builder implements SelectDistinctTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private long initBits = 0x1L;

    private TableSpec parent;
<<<<<<< HEAD
    private final List<Selectable> columns = new ArrayList<Selectable>();
=======
    private List<Selectable> columns = new ArrayList<Selectable>();
>>>>>>> main

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.SelectableTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SelectableTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.SingleParentTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SingleParentTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.SelectDistinctTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SelectDistinctTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof SelectableTable) {
        SelectableTable instance = (SelectableTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          addAllColumns(instance.columns());
          bits |= 0x2L;
        }
      }
      if (object instanceof SingleParentTable) {
        SingleParentTable instance = (SingleParentTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
      if (object instanceof SelectDistinctTable) {
        SelectDistinctTable instance = (SelectDistinctTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          addAllColumns(instance.columns());
          bits |= 0x2L;
        }
      }
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link SelectDistinctTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
<<<<<<< HEAD
      checkNotIsSet(parentIsSet(), "parent");
=======
>>>>>>> main
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
      return this;
    }

    /**
     * Adds one element to {@link SelectDistinctTable#columns() columns} list.
     * @param element A columns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(Selectable element) {
      this.columns.add(Objects.requireNonNull(element, "columns element"));
      return this;
    }

    /**
     * Adds elements to {@link SelectDistinctTable#columns() columns} list.
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
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link SelectDistinctTable#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder columns(Iterable<? extends Selectable> elements) {
      this.columns.clear();
      return addAllColumns(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link SelectDistinctTable#columns() columns} list.
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
     * Builds a new {@link ImmutableSelectDistinctTable ImmutableSelectDistinctTable}.
     * @return An immutable instance of SelectDistinctTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSelectDistinctTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
      return ImmutableSelectDistinctTable.validate(new ImmutableSelectDistinctTable(parent, createUnmodifiableList(true, columns)));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of SelectDistinctTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSelectDistinctTable(parent, createUnmodifiableList(true, columns));
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
<<<<<<< HEAD
      if (!parentIsSet()) attributes.add("parent");
=======
      if ((initBits & INIT_BIT_PARENT) != 0) attributes.add("parent");
>>>>>>> main
      return "Cannot build SelectDistinctTable, some of required attributes are not set " + attributes;
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
