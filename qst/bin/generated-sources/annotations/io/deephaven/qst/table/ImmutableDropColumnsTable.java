package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
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
 * Immutable implementation of {@link DropColumnsTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDropColumnsTable.builder()}.
 */
@Generated(from = "DropColumnsTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableDropColumnsTable extends DropColumnsTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<ColumnName> dropColumns;
  private transient final int hashCode;
=======
public final class ImmutableDropColumnsTable extends DropColumnsTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<ColumnName> dropColumns;
>>>>>>> main

  private ImmutableDropColumnsTable(TableSpec parent, List<ColumnName> dropColumns) {
    this.parent = parent;
    this.dropColumns = dropColumns;
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
   * @return The value of the {@code dropColumns} attribute
   */
  @Override
  public List<ColumnName> dropColumns() {
    return dropColumns;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DropColumnsTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDropColumnsTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
<<<<<<< HEAD
    return validate(new ImmutableDropColumnsTable(newValue, this.dropColumns));
=======
    return new ImmutableDropColumnsTable(newValue, this.dropColumns);
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link DropColumnsTable#dropColumns() dropColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDropColumnsTable withDropColumns(ColumnName... elements) {
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
<<<<<<< HEAD
    return validate(new ImmutableDropColumnsTable(this.parent, newValue));
=======
    return new ImmutableDropColumnsTable(this.parent, newValue);
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link DropColumnsTable#dropColumns() dropColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of dropColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDropColumnsTable withDropColumns(Iterable<? extends ColumnName> elements) {
    if (this.dropColumns == elements) return this;
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
<<<<<<< HEAD
    return validate(new ImmutableDropColumnsTable(this.parent, newValue));
=======
    return new ImmutableDropColumnsTable(this.parent, newValue);
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDropColumnsTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDropColumnsTable
        && equalTo(0, (ImmutableDropColumnsTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableDropColumnsTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
=======
>>>>>>> main
    return depth == another.depth
        && parent.equals(another.parent)
        && dropColumns.equals(another.dropColumns);
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code dropColumns}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code parent}, {@code dropColumns}.
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
    h += (h << 5) + dropColumns.hashCode();
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableDropColumnsTable, WeakReference<ImmutableDropColumnsTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableDropColumnsTable validate(ImmutableDropColumnsTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableDropColumnsTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableDropColumnsTable interned = reference != null ? reference.get() : null;
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
   * Creates an immutable copy of a {@link DropColumnsTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DropColumnsTable instance
   */
  public static ImmutableDropColumnsTable copyOf(DropColumnsTable instance) {
    if (instance instanceof ImmutableDropColumnsTable) {
      return (ImmutableDropColumnsTable) instance;
    }
    return ImmutableDropColumnsTable.builder()
<<<<<<< HEAD
        .parent(instance.parent())
        .addAllDropColumns(instance.dropColumns())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDropColumnsTable ImmutableDropColumnsTable}.
   * <pre>
   * ImmutableDropColumnsTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link DropColumnsTable#parent() parent}
   *    .addDropColumns|addAllDropColumns(io.deephaven.api.ColumnName) // {@link DropColumnsTable#dropColumns() dropColumns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableDropColumnsTable builder
   */
  public static ImmutableDropColumnsTable.Builder builder() {
    return new ImmutableDropColumnsTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDropColumnsTable ImmutableDropColumnsTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DropColumnsTable", generator = "Immutables")
  public static final class Builder implements DropColumnsTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private long initBits = 0x1L;

    private TableSpec parent;
<<<<<<< HEAD
    private final List<ColumnName> dropColumns = new ArrayList<ColumnName>();
=======
    private List<ColumnName> dropColumns = new ArrayList<ColumnName>();
>>>>>>> main

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.DropColumnsTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(DropColumnsTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof SingleParentTable) {
        SingleParentTable instance = (SingleParentTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
      if (object instanceof DropColumnsTable) {
        DropColumnsTable instance = (DropColumnsTable) object;
        addAllDropColumns(instance.dropColumns());
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link DropColumnsTable#parent() parent} attribute.
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
     * Adds one element to {@link DropColumnsTable#dropColumns() dropColumns} list.
     * @param element A dropColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addDropColumns(ColumnName element) {
      this.dropColumns.add(Objects.requireNonNull(element, "dropColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link DropColumnsTable#dropColumns() dropColumns} list.
     * @param elements An array of dropColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addDropColumns(ColumnName... elements) {
      for (ColumnName element : elements) {
        this.dropColumns.add(Objects.requireNonNull(element, "dropColumns element"));
      }
      return this;
    }


    /**
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link DropColumnsTable#dropColumns() dropColumns} list.
     * @param elements An iterable of dropColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder dropColumns(Iterable<? extends ColumnName> elements) {
      this.dropColumns.clear();
      return addAllDropColumns(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link DropColumnsTable#dropColumns() dropColumns} list.
     * @param elements An iterable of dropColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllDropColumns(Iterable<? extends ColumnName> elements) {
      for (ColumnName element : elements) {
        this.dropColumns.add(Objects.requireNonNull(element, "dropColumns element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableDropColumnsTable ImmutableDropColumnsTable}.
     * @return An immutable instance of DropColumnsTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDropColumnsTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
      return ImmutableDropColumnsTable.validate(new ImmutableDropColumnsTable(parent, createUnmodifiableList(true, dropColumns)));
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of DropColumnsTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableDropColumnsTable(parent, createUnmodifiableList(true, dropColumns));
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
<<<<<<< HEAD
      if (!parentIsSet()) attributes.add("parent");
=======
      if ((initBits & INIT_BIT_PARENT) != 0) attributes.add("parent");
>>>>>>> main
      return "Cannot build DropColumnsTable, some of required attributes are not set " + attributes;
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
