package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
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
 * Immutable implementation of {@link UngroupTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableUngroupTable.builder()}.
 */
@Generated(from = "UngroupTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableUngroupTable extends UngroupTable {
  private final int depth;
  private final TableSpec parent;
  private final List<ColumnName> ungroupColumns;
  private final boolean nullFill;
  private final int hashCode;

  private ImmutableUngroupTable(ImmutableUngroupTable.Builder builder) {
    this.parent = builder.parent;
    this.ungroupColumns = createUnmodifiableList(true, builder.ungroupColumns);
    if (builder.nullFillIsSet()) {
      initShim.nullFill(builder.nullFill);
    }
    this.depth = initShim.depth();
    this.nullFill = initShim.nullFill();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private ImmutableUngroupTable(
      TableSpec parent,
      List<ColumnName> ungroupColumns,
      boolean nullFill) {
    this.parent = parent;
    this.ungroupColumns = ungroupColumns;
    initShim.nullFill(nullFill);
    this.depth = initShim.depth();
    this.nullFill = initShim.nullFill();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "UngroupTable", generator = "Immutables")
  private final class InitShim {
    private byte depthBuildStage = STAGE_UNINITIALIZED;
    private int depth;

    int depth() {
      if (depthBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (depthBuildStage == STAGE_UNINITIALIZED) {
        depthBuildStage = STAGE_INITIALIZING;
        this.depth = ImmutableUngroupTable.super.depth();
        depthBuildStage = STAGE_INITIALIZED;
      }
      return this.depth;
    }

    private byte nullFillBuildStage = STAGE_UNINITIALIZED;
    private boolean nullFill;

    boolean nullFill() {
      if (nullFillBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (nullFillBuildStage == STAGE_UNINITIALIZED) {
        nullFillBuildStage = STAGE_INITIALIZING;
        this.nullFill = ImmutableUngroupTable.super.nullFill();
        nullFillBuildStage = STAGE_INITIALIZED;
      }
      return this.nullFill;
    }

    void nullFill(boolean nullFill) {
      this.nullFill = nullFill;
      nullFillBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (depthBuildStage == STAGE_INITIALIZING) attributes.add("depth");
      if (nullFillBuildStage == STAGE_INITIALIZING) attributes.add("nullFill");
      return "Cannot build UngroupTable, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.depth()
        : this.depth;
  }

  /**
   * @return The value of the {@code parent} attribute
   */
  @Override
  public TableSpec parent() {
    return parent;
  }

  /**
   * @return The value of the {@code ungroupColumns} attribute
   */
  @Override
  public List<ColumnName> ungroupColumns() {
    return ungroupColumns;
  }

  /**
   * @return The value of the {@code nullFill} attribute
   */
  @Override
  public boolean nullFill() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.nullFill()
        : this.nullFill;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link UngroupTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableUngroupTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableUngroupTable(newValue, this.ungroupColumns, this.nullFill));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UngroupTable#ungroupColumns() ungroupColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUngroupTable withUngroupColumns(ColumnName... elements) {
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableUngroupTable(this.parent, newValue, this.nullFill));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link UngroupTable#ungroupColumns() ungroupColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of ungroupColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUngroupTable withUngroupColumns(Iterable<? extends ColumnName> elements) {
    if (this.ungroupColumns == elements) return this;
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableUngroupTable(this.parent, newValue, this.nullFill));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link UngroupTable#nullFill() nullFill} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for nullFill
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableUngroupTable withNullFill(boolean value) {
    if (this.nullFill == value) return this;
    return validate(new ImmutableUngroupTable(this.parent, this.ungroupColumns, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableUngroupTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableUngroupTable
        && equalTo(0, (ImmutableUngroupTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableUngroupTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && ungroupColumns.equals(another.ungroupColumns)
        && nullFill == another.nullFill;
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code ungroupColumns}, {@code nullFill}.
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
    h += (h << 5) + ungroupColumns.hashCode();
    h += (h << 5) + Boolean.hashCode(nullFill);
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableUngroupTable, WeakReference<ImmutableUngroupTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableUngroupTable validate(ImmutableUngroupTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableUngroupTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableUngroupTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link UngroupTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable UngroupTable instance
   */
  public static ImmutableUngroupTable copyOf(UngroupTable instance) {
    if (instance instanceof ImmutableUngroupTable) {
      return (ImmutableUngroupTable) instance;
    }
    return ImmutableUngroupTable.builder()
        .parent(instance.parent())
        .addAllUngroupColumns(instance.ungroupColumns())
        .nullFill(instance.nullFill())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableUngroupTable(this.parent, this.ungroupColumns, this.nullFill));
  }

  /**
   * Creates a builder for {@link ImmutableUngroupTable ImmutableUngroupTable}.
   * <pre>
   * ImmutableUngroupTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link UngroupTable#parent() parent}
   *    .addUngroupColumns|addAllUngroupColumns(io.deephaven.api.ColumnName) // {@link UngroupTable#ungroupColumns() ungroupColumns} elements
   *    .nullFill(boolean) // optional {@link UngroupTable#nullFill() nullFill}
   *    .build();
   * </pre>
   * @return A new ImmutableUngroupTable builder
   */
  public static ImmutableUngroupTable.Builder builder() {
    return new ImmutableUngroupTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableUngroupTable ImmutableUngroupTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "UngroupTable", generator = "Immutables")
  public static final class Builder implements UngroupTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long OPT_BIT_NULL_FILL = 0x1L;
    private long initBits = 0x1L;
    private long optBits;

    private TableSpec parent;
    private final List<ColumnName> ungroupColumns = new ArrayList<ColumnName>();
    private boolean nullFill;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link UngroupTable#parent() parent} attribute.
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
     * Adds one element to {@link UngroupTable#ungroupColumns() ungroupColumns} list.
     * @param element A ungroupColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addUngroupColumns(ColumnName element) {
      this.ungroupColumns.add(Objects.requireNonNull(element, "ungroupColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link UngroupTable#ungroupColumns() ungroupColumns} list.
     * @param elements An array of ungroupColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addUngroupColumns(ColumnName... elements) {
      for (ColumnName element : elements) {
        this.ungroupColumns.add(Objects.requireNonNull(element, "ungroupColumns element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link UngroupTable#ungroupColumns() ungroupColumns} list.
     * @param elements An iterable of ungroupColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllUngroupColumns(Iterable<? extends ColumnName> elements) {
      for (ColumnName element : elements) {
        this.ungroupColumns.add(Objects.requireNonNull(element, "ungroupColumns element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link UngroupTable#nullFill() nullFill} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link UngroupTable#nullFill() nullFill}.</em>
     * @param nullFill The value for nullFill 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder nullFill(boolean nullFill) {
      checkNotIsSet(nullFillIsSet(), "nullFill");
      this.nullFill = nullFill;
      optBits |= OPT_BIT_NULL_FILL;
      return this;
    }

    /**
     * Builds a new {@link ImmutableUngroupTable ImmutableUngroupTable}.
     * @return An immutable instance of UngroupTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableUngroupTable build() {
      checkRequiredAttributes();
      return ImmutableUngroupTable.validate(new ImmutableUngroupTable(this));
    }

    private boolean nullFillIsSet() {
      return (optBits & OPT_BIT_NULL_FILL) != 0;
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of UngroupTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
      return "Cannot build UngroupTable, some of required attributes are not set " + attributes;
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
