package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import java.io.ObjectStreamException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link UpdateByTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableUpdateByTable.builder()}.
 */
@Generated(from = "UpdateByTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableUpdateByTable extends UpdateByTable {
  private final int depth;
  private final TableSpec parent;
  private final List<ColumnName> groupByColumns;
  private final UpdateByControl control;
  private final List<UpdateByOperation> operations;
  private final int hashCode;

  private ImmutableUpdateByTable(
      TableSpec parent,
      List<ColumnName> groupByColumns,
      UpdateByControl control,
      List<UpdateByOperation> operations) {
    this.parent = parent;
    this.groupByColumns = groupByColumns;
    this.control = control;
    this.operations = operations;
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
   * @return The value of the {@code groupByColumns} attribute
   */
  @Override
  public List<ColumnName> groupByColumns() {
    return groupByColumns;
  }

  /**
   * @return The value of the {@code control} attribute
   */
  @Override
  public Optional<UpdateByControl> control() {
    return Optional.ofNullable(control);
  }

  /**
   * @return The value of the {@code operations} attribute
   */
  @Override
  public List<UpdateByOperation> operations() {
    return operations;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link UpdateByTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableUpdateByTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableUpdateByTable(newValue, this.groupByColumns, this.control, this.operations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UpdateByTable#groupByColumns() groupByColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByTable withGroupByColumns(ColumnName... elements) {
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableUpdateByTable(this.parent, newValue, this.control, this.operations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UpdateByTable#groupByColumns() groupByColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of groupByColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByTable withGroupByColumns(Iterable<? extends ColumnName> elements) {
    if (this.groupByColumns == elements) return this;
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableUpdateByTable(this.parent, newValue, this.control, this.operations));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByTable#control() control} attribute.
   * @param value The value for control
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByTable withControl(UpdateByControl value) {
    UpdateByControl newValue = Objects.requireNonNull(value, "control");
    if (this.control == newValue) return this;
    return validate(new ImmutableUpdateByTable(this.parent, this.groupByColumns, newValue, this.operations));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByTable#control() control} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for control
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableUpdateByTable withControl(Optional<? extends UpdateByControl> optional) {
    UpdateByControl value = optional.orElse(null);
    if (this.control == value) return this;
    return validate(new ImmutableUpdateByTable(this.parent, this.groupByColumns, value, this.operations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UpdateByTable#operations() operations}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByTable withOperations(UpdateByOperation... elements) {
    List<UpdateByOperation> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableUpdateByTable(this.parent, this.groupByColumns, this.control, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UpdateByTable#operations() operations}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of operations elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByTable withOperations(Iterable<? extends UpdateByOperation> elements) {
    if (this.operations == elements) return this;
    List<UpdateByOperation> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableUpdateByTable(this.parent, this.groupByColumns, this.control, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableUpdateByTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableUpdateByTable
        && equalTo(0, (ImmutableUpdateByTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableUpdateByTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && groupByColumns.equals(another.groupByColumns)
        && Objects.equals(control, another.control)
        && operations.equals(another.operations);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code groupByColumns}, {@code control}, {@code operations}.
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
    h += (h << 5) + groupByColumns.hashCode();
    h += (h << 5) + Objects.hashCode(control);
    h += (h << 5) + operations.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableUpdateByTable, WeakReference<ImmutableUpdateByTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableUpdateByTable validate(ImmutableUpdateByTable instance) {
    instance.checkNumOperations();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableUpdateByTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableUpdateByTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link UpdateByTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable UpdateByTable instance
   */
  public static ImmutableUpdateByTable copyOf(UpdateByTable instance) {
    if (instance instanceof ImmutableUpdateByTable) {
      return (ImmutableUpdateByTable) instance;
    }
    return ImmutableUpdateByTable.builder()
        .parent(instance.parent())
        .addAllGroupByColumns(instance.groupByColumns())
        .control(instance.control())
        .addAllOperations(instance.operations())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableUpdateByTable(this.parent, this.groupByColumns, this.control, this.operations));
  }

  /**
   * Creates a builder for {@link ImmutableUpdateByTable ImmutableUpdateByTable}.
   * <pre>
   * ImmutableUpdateByTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link UpdateByTable#parent() parent}
   *    .addGroupByColumns|addAllGroupByColumns(io.deephaven.api.ColumnName) // {@link UpdateByTable#groupByColumns() groupByColumns} elements
   *    .control(io.deephaven.api.updateby.UpdateByControl) // optional {@link UpdateByTable#control() control}
   *    .addOperations|addAllOperations(io.deephaven.api.updateby.UpdateByOperation) // {@link UpdateByTable#operations() operations} elements
   *    .build();
   * </pre>
   * @return A new ImmutableUpdateByTable builder
   */
  public static ImmutableUpdateByTable.Builder builder() {
    return new ImmutableUpdateByTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableUpdateByTable ImmutableUpdateByTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "UpdateByTable", generator = "Immutables")
  public static final class Builder implements UpdateByTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long OPT_BIT_CONTROL = 0x1L;
    private long initBits = 0x1L;
    private long optBits;

    private TableSpec parent;
    private final List<ColumnName> groupByColumns = new ArrayList<ColumnName>();
    private UpdateByControl control;
    private final List<UpdateByOperation> operations = new ArrayList<UpdateByOperation>();

    private Builder() {
    }

    /**
     * Initializes the value for the {@link UpdateByTable#parent() parent} attribute.
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
     * Adds one element to {@link UpdateByTable#groupByColumns() groupByColumns} list.
     * @param element A groupByColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addGroupByColumns(ColumnName element) {
      this.groupByColumns.add(Objects.requireNonNull(element, "groupByColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link UpdateByTable#groupByColumns() groupByColumns} list.
     * @param elements An array of groupByColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addGroupByColumns(ColumnName... elements) {
      for (ColumnName element : elements) {
        this.groupByColumns.add(Objects.requireNonNull(element, "groupByColumns element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link UpdateByTable#groupByColumns() groupByColumns} list.
     * @param elements An iterable of groupByColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllGroupByColumns(Iterable<? extends ColumnName> elements) {
      for (ColumnName element : elements) {
        this.groupByColumns.add(Objects.requireNonNull(element, "groupByColumns element"));
      }
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByTable#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for chained invocation
     */
    public final Builder control(UpdateByControl control) {
      checkNotIsSet(controlIsSet(), "control");
      this.control = Objects.requireNonNull(control, "control");
      optBits |= OPT_BIT_CONTROL;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByTable#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder control(Optional<? extends UpdateByControl> control) {
      checkNotIsSet(controlIsSet(), "control");
      this.control = control.orElse(null);
      optBits |= OPT_BIT_CONTROL;
      return this;
    }

    /**
     * Adds one element to {@link UpdateByTable#operations() operations} list.
     * @param element A operations element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addOperations(UpdateByOperation element) {
      this.operations.add(Objects.requireNonNull(element, "operations element"));
      return this;
    }

    /**
     * Adds elements to {@link UpdateByTable#operations() operations} list.
     * @param elements An array of operations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addOperations(UpdateByOperation... elements) {
      for (UpdateByOperation element : elements) {
        this.operations.add(Objects.requireNonNull(element, "operations element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link UpdateByTable#operations() operations} list.
     * @param elements An iterable of operations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllOperations(Iterable<? extends UpdateByOperation> elements) {
      for (UpdateByOperation element : elements) {
        this.operations.add(Objects.requireNonNull(element, "operations element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableUpdateByTable ImmutableUpdateByTable}.
     * @return An immutable instance of UpdateByTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableUpdateByTable build() {
      checkRequiredAttributes();
      return ImmutableUpdateByTable.validate(new ImmutableUpdateByTable(
          parent,
          createUnmodifiableList(true, groupByColumns),
          control,
          createUnmodifiableList(true, operations)));
    }

    private boolean controlIsSet() {
      return (optBits & OPT_BIT_CONTROL) != 0;
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of UpdateByTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
      return "Cannot build UpdateByTable, some of required attributes are not set " + attributes;
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
