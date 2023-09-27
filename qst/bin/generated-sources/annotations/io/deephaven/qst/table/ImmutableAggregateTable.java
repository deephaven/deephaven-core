package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
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
import java.util.Optional;
import java.util.WeakHashMap;
=======
import java.util.Objects;
import java.util.Optional;
>>>>>>> main
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggregateTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggregateTable.builder()}.
 */
@Generated(from = "AggregateTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableAggregateTable extends AggregateTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<ColumnName> groupByColumns;
  private final List<Aggregation> aggregations;
  private final boolean preserveEmpty;
  private final TableSpec initialGroups;
  private transient final int hashCode;

  private ImmutableAggregateTable(ImmutableAggregateTable.Builder builder) {
    this.parent = builder.parent;
    this.groupByColumns = createUnmodifiableList(true, builder.groupByColumns);
=======
public final class ImmutableAggregateTable extends AggregateTable {
  private transient final int depth;
  private final List<ColumnName> groupByColumns;
  private final TableSpec parent;
  private final List<Aggregation> aggregations;
  private final boolean preserveEmpty;
  private final TableSpec initialGroups;

  private ImmutableAggregateTable(ImmutableAggregateTable.Builder builder) {
    this.groupByColumns = createUnmodifiableList(true, builder.groupByColumns);
    this.parent = builder.parent;
>>>>>>> main
    this.aggregations = createUnmodifiableList(true, builder.aggregations);
    this.initialGroups = builder.initialGroups;
    if (builder.preserveEmptyIsSet()) {
      initShim.preserveEmpty(builder.preserveEmpty);
    }
    this.depth = initShim.depth();
    this.preserveEmpty = initShim.preserveEmpty();
<<<<<<< HEAD
    this.hashCode = computeHashCode();
=======
>>>>>>> main
    this.initShim = null;
  }

  private ImmutableAggregateTable(
<<<<<<< HEAD
      TableSpec parent,
      List<ColumnName> groupByColumns,
      List<Aggregation> aggregations,
      boolean preserveEmpty,
      TableSpec initialGroups) {
    this.parent = parent;
    this.groupByColumns = groupByColumns;
=======
      List<ColumnName> groupByColumns,
      TableSpec parent,
      List<Aggregation> aggregations,
      boolean preserveEmpty,
      TableSpec initialGroups) {
    this.groupByColumns = groupByColumns;
    this.parent = parent;
>>>>>>> main
    this.aggregations = aggregations;
    initShim.preserveEmpty(preserveEmpty);
    this.initialGroups = initialGroups;
    this.depth = initShim.depth();
    this.preserveEmpty = initShim.preserveEmpty();
<<<<<<< HEAD
    this.hashCode = computeHashCode();
=======
>>>>>>> main
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "AggregateTable", generator = "Immutables")
  private final class InitShim {
    private byte depthBuildStage = STAGE_UNINITIALIZED;
    private int depth;

    int depth() {
      if (depthBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (depthBuildStage == STAGE_UNINITIALIZED) {
        depthBuildStage = STAGE_INITIALIZING;
        this.depth = ImmutableAggregateTable.super.depth();
        depthBuildStage = STAGE_INITIALIZED;
      }
      return this.depth;
    }

    private byte preserveEmptyBuildStage = STAGE_UNINITIALIZED;
    private boolean preserveEmpty;

    boolean preserveEmpty() {
      if (preserveEmptyBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (preserveEmptyBuildStage == STAGE_UNINITIALIZED) {
        preserveEmptyBuildStage = STAGE_INITIALIZING;
        this.preserveEmpty = ImmutableAggregateTable.super.preserveEmpty();
        preserveEmptyBuildStage = STAGE_INITIALIZED;
      }
      return this.preserveEmpty;
    }

    void preserveEmpty(boolean preserveEmpty) {
      this.preserveEmpty = preserveEmpty;
      preserveEmptyBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (depthBuildStage == STAGE_INITIALIZING) attributes.add("depth");
      if (preserveEmptyBuildStage == STAGE_INITIALIZING) attributes.add("preserveEmpty");
      return "Cannot build AggregateTable, attribute initializers form cycle " + attributes;
    }
  }

  /**
<<<<<<< HEAD
   * @return The computed-at-construction value of the {@code depth} attribute
=======
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
>>>>>>> main
   */
  @Override
  public int depth() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.depth()
        : this.depth;
  }

  /**
<<<<<<< HEAD
   * @return The value of the {@code parent} attribute
   */
  @Override
  public TableSpec parent() {
    return parent;
  }

  /**
=======
>>>>>>> main
   * @return The value of the {@code groupByColumns} attribute
   */
  @Override
  public List<ColumnName> groupByColumns() {
    return groupByColumns;
  }

  /**
<<<<<<< HEAD
=======
   * @return The value of the {@code parent} attribute
   */
  @Override
  public TableSpec parent() {
    return parent;
  }

  /**
>>>>>>> main
   * @return The value of the {@code aggregations} attribute
   */
  @Override
  public List<Aggregation> aggregations() {
    return aggregations;
  }

  /**
   * @return The value of the {@code preserveEmpty} attribute
   */
  @Override
  public boolean preserveEmpty() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.preserveEmpty()
        : this.preserveEmpty;
  }

  /**
   * @return The value of the {@code initialGroups} attribute
   */
  @Override
  public Optional<TableSpec> initialGroups() {
    return Optional.ofNullable(initialGroups);
  }

  /**
<<<<<<< HEAD
   * Copy the current immutable object by setting a value for the {@link AggregateTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggregateTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableAggregateTable(newValue, this.groupByColumns, this.aggregations, this.preserveEmpty, this.initialGroups));
  }

  /**
=======
>>>>>>> main
   * Copy the current immutable object with elements that replace the content of {@link AggregateTable#groupByColumns() groupByColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateTable withGroupByColumns(ColumnName... elements) {
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, newValue, this.aggregations, this.preserveEmpty, this.initialGroups));
=======
    return validate(new ImmutableAggregateTable(newValue, this.parent, this.aggregations, this.preserveEmpty, this.initialGroups));
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggregateTable#groupByColumns() groupByColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of groupByColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateTable withGroupByColumns(Iterable<? extends ColumnName> elements) {
    if (this.groupByColumns == elements) return this;
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, newValue, this.aggregations, this.preserveEmpty, this.initialGroups));
=======
    return validate(new ImmutableAggregateTable(newValue, this.parent, this.aggregations, this.preserveEmpty, this.initialGroups));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggregateTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggregateTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return validate(new ImmutableAggregateTable(this.groupByColumns, newValue, this.aggregations, this.preserveEmpty, this.initialGroups));
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggregateTable#aggregations() aggregations}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateTable withAggregations(Aggregation... elements) {
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, this.groupByColumns, newValue, this.preserveEmpty, this.initialGroups));
=======
    return validate(new ImmutableAggregateTable(this.groupByColumns, this.parent, newValue, this.preserveEmpty, this.initialGroups));
>>>>>>> main
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggregateTable#aggregations() aggregations}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of aggregations elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateTable withAggregations(Iterable<? extends Aggregation> elements) {
    if (this.aggregations == elements) return this;
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, this.groupByColumns, newValue, this.preserveEmpty, this.initialGroups));
=======
    return validate(new ImmutableAggregateTable(this.groupByColumns, this.parent, newValue, this.preserveEmpty, this.initialGroups));
>>>>>>> main
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggregateTable#preserveEmpty() preserveEmpty} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for preserveEmpty
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggregateTable withPreserveEmpty(boolean value) {
    if (this.preserveEmpty == value) return this;
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, this.groupByColumns, this.aggregations, value, this.initialGroups));
=======
    return validate(new ImmutableAggregateTable(this.groupByColumns, this.parent, this.aggregations, value, this.initialGroups));
>>>>>>> main
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link AggregateTable#initialGroups() initialGroups} attribute.
   * @param value The value for initialGroups
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateTable withInitialGroups(TableSpec value) {
    TableSpec newValue = Objects.requireNonNull(value, "initialGroups");
    if (this.initialGroups == newValue) return this;
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, this.groupByColumns, this.aggregations, this.preserveEmpty, newValue));
=======
    return validate(new ImmutableAggregateTable(this.groupByColumns, this.parent, this.aggregations, this.preserveEmpty, newValue));
>>>>>>> main
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link AggregateTable#initialGroups() initialGroups} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for initialGroups
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableAggregateTable withInitialGroups(Optional<? extends TableSpec> optional) {
    TableSpec value = optional.orElse(null);
    if (this.initialGroups == value) return this;
<<<<<<< HEAD
    return validate(new ImmutableAggregateTable(this.parent, this.groupByColumns, this.aggregations, this.preserveEmpty, value));
=======
    return validate(new ImmutableAggregateTable(this.groupByColumns, this.parent, this.aggregations, this.preserveEmpty, value));
>>>>>>> main
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggregateTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggregateTable
        && equalTo(0, (ImmutableAggregateTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggregateTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && parent.equals(another.parent)
        && groupByColumns.equals(another.groupByColumns)
=======
    return depth == another.depth
        && groupByColumns.equals(another.groupByColumns)
        && parent.equals(another.parent)
>>>>>>> main
        && aggregations.equals(another.aggregations)
        && preserveEmpty == another.preserveEmpty
        && Objects.equals(initialGroups, another.initialGroups);
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code parent}, {@code groupByColumns}, {@code aggregations}, {@code preserveEmpty}, {@code initialGroups}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code groupByColumns}, {@code parent}, {@code aggregations}, {@code preserveEmpty}, {@code initialGroups}.
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
    h += (h << 5) + depth;
    h += (h << 5) + parent.hashCode();
    h += (h << 5) + groupByColumns.hashCode();
=======
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + groupByColumns.hashCode();
    h += (h << 5) + parent.hashCode();
>>>>>>> main
    h += (h << 5) + aggregations.hashCode();
    h += (h << 5) + Boolean.hashCode(preserveEmpty);
    h += (h << 5) + Objects.hashCode(initialGroups);
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableAggregateTable, WeakReference<ImmutableAggregateTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableAggregateTable validate(ImmutableAggregateTable instance) {
    instance.checkInitialGroups();
    instance.checkNumAggs();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableAggregateTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableAggregateTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
=======
  private static ImmutableAggregateTable validate(ImmutableAggregateTable instance) {
    instance.checkInitialGroups();
    instance.checkNumAggs();
    return instance;
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link AggregateTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggregateTable instance
   */
  public static ImmutableAggregateTable copyOf(AggregateTable instance) {
    if (instance instanceof ImmutableAggregateTable) {
      return (ImmutableAggregateTable) instance;
    }
    return ImmutableAggregateTable.builder()
<<<<<<< HEAD
        .parent(instance.parent())
        .addAllGroupByColumns(instance.groupByColumns())
        .addAllAggregations(instance.aggregations())
        .preserveEmpty(instance.preserveEmpty())
        .initialGroups(instance.initialGroups())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggregateTable ImmutableAggregateTable}.
   * <pre>
   * ImmutableAggregateTable.builder()
<<<<<<< HEAD
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link AggregateTable#parent() parent}
   *    .addGroupByColumns|addAllGroupByColumns(io.deephaven.api.ColumnName) // {@link AggregateTable#groupByColumns() groupByColumns} elements
=======
   *    .addGroupByColumns|addAllGroupByColumns(io.deephaven.api.ColumnName) // {@link AggregateTable#groupByColumns() groupByColumns} elements
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link AggregateTable#parent() parent}
>>>>>>> main
   *    .addAggregations|addAllAggregations(io.deephaven.api.agg.Aggregation) // {@link AggregateTable#aggregations() aggregations} elements
   *    .preserveEmpty(boolean) // optional {@link AggregateTable#preserveEmpty() preserveEmpty}
   *    .initialGroups(io.deephaven.qst.table.TableSpec) // optional {@link AggregateTable#initialGroups() initialGroups}
   *    .build();
   * </pre>
   * @return A new ImmutableAggregateTable builder
   */
  public static ImmutableAggregateTable.Builder builder() {
    return new ImmutableAggregateTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggregateTable ImmutableAggregateTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggregateTable", generator = "Immutables")
  public static final class Builder implements AggregateTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long OPT_BIT_PRESERVE_EMPTY = 0x1L;
<<<<<<< HEAD
    private static final long OPT_BIT_INITIAL_GROUPS = 0x2L;
    private long initBits = 0x1L;
    private long optBits;

    private TableSpec parent;
    private final List<ColumnName> groupByColumns = new ArrayList<ColumnName>();
    private final List<Aggregation> aggregations = new ArrayList<Aggregation>();
=======
    private long initBits = 0x1L;
    private long optBits;

    private List<ColumnName> groupByColumns = new ArrayList<ColumnName>();
    private TableSpec parent;
    private List<Aggregation> aggregations = new ArrayList<Aggregation>();
>>>>>>> main
    private boolean preserveEmpty;
    private TableSpec initialGroups;

    private Builder() {
    }

    /**
<<<<<<< HEAD
     * Initializes the value for the {@link AggregateTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
      checkNotIsSet(parentIsSet(), "parent");
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
=======
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.ByTableBase} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ByTableBase instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
>>>>>>> main
      return this;
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.AggregateTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggregateTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof ByTableBase) {
        ByTableBase instance = (ByTableBase) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          addAllGroupByColumns(instance.groupByColumns());
          bits |= 0x2L;
        }
      }
      if (object instanceof AggregateTable) {
        AggregateTable instance = (AggregateTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
        preserveEmpty(instance.preserveEmpty());
        addAllAggregations(instance.aggregations());
        Optional<TableSpec> initialGroupsOptional = instance.initialGroups();
        if (initialGroupsOptional.isPresent()) {
          initialGroups(initialGroupsOptional);
        }
        if ((bits & 0x2L) == 0) {
          addAllGroupByColumns(instance.groupByColumns());
          bits |= 0x2L;
        }
      }
    }

    /**
>>>>>>> main
     * Adds one element to {@link AggregateTable#groupByColumns() groupByColumns} list.
     * @param element A groupByColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addGroupByColumns(ColumnName element) {
      this.groupByColumns.add(Objects.requireNonNull(element, "groupByColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link AggregateTable#groupByColumns() groupByColumns} list.
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
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link AggregateTable#groupByColumns() groupByColumns} list.
     * @param elements An iterable of groupByColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder groupByColumns(Iterable<? extends ColumnName> elements) {
      this.groupByColumns.clear();
      return addAllGroupByColumns(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link AggregateTable#groupByColumns() groupByColumns} list.
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
<<<<<<< HEAD
=======
     * Initializes the value for the {@link AggregateTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
      return this;
    }

    /**
>>>>>>> main
     * Adds one element to {@link AggregateTable#aggregations() aggregations} list.
     * @param element A aggregations element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAggregations(Aggregation element) {
      this.aggregations.add(Objects.requireNonNull(element, "aggregations element"));
      return this;
    }

    /**
     * Adds elements to {@link AggregateTable#aggregations() aggregations} list.
     * @param elements An array of aggregations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAggregations(Aggregation... elements) {
      for (Aggregation element : elements) {
        this.aggregations.add(Objects.requireNonNull(element, "aggregations element"));
      }
      return this;
    }


    /**
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link AggregateTable#aggregations() aggregations} list.
     * @param elements An iterable of aggregations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder aggregations(Iterable<? extends Aggregation> elements) {
      this.aggregations.clear();
      return addAllAggregations(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link AggregateTable#aggregations() aggregations} list.
     * @param elements An iterable of aggregations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllAggregations(Iterable<? extends Aggregation> elements) {
      for (Aggregation element : elements) {
        this.aggregations.add(Objects.requireNonNull(element, "aggregations element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link AggregateTable#preserveEmpty() preserveEmpty} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link AggregateTable#preserveEmpty() preserveEmpty}.</em>
     * @param preserveEmpty The value for preserveEmpty 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder preserveEmpty(boolean preserveEmpty) {
<<<<<<< HEAD
      checkNotIsSet(preserveEmptyIsSet(), "preserveEmpty");
=======
>>>>>>> main
      this.preserveEmpty = preserveEmpty;
      optBits |= OPT_BIT_PRESERVE_EMPTY;
      return this;
    }

    /**
     * Initializes the optional value {@link AggregateTable#initialGroups() initialGroups} to initialGroups.
     * @param initialGroups The value for initialGroups
     * @return {@code this} builder for chained invocation
     */
    public final Builder initialGroups(TableSpec initialGroups) {
<<<<<<< HEAD
      checkNotIsSet(initialGroupsIsSet(), "initialGroups");
      this.initialGroups = Objects.requireNonNull(initialGroups, "initialGroups");
      optBits |= OPT_BIT_INITIAL_GROUPS;
=======
      this.initialGroups = Objects.requireNonNull(initialGroups, "initialGroups");
>>>>>>> main
      return this;
    }

    /**
     * Initializes the optional value {@link AggregateTable#initialGroups() initialGroups} to initialGroups.
     * @param initialGroups The value for initialGroups
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder initialGroups(Optional<? extends TableSpec> initialGroups) {
<<<<<<< HEAD
      checkNotIsSet(initialGroupsIsSet(), "initialGroups");
      this.initialGroups = initialGroups.orElse(null);
      optBits |= OPT_BIT_INITIAL_GROUPS;
=======
      this.initialGroups = initialGroups.orElse(null);
>>>>>>> main
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggregateTable ImmutableAggregateTable}.
     * @return An immutable instance of AggregateTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggregateTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
>>>>>>> main
      return ImmutableAggregateTable.validate(new ImmutableAggregateTable(this));
    }

    private boolean preserveEmptyIsSet() {
      return (optBits & OPT_BIT_PRESERVE_EMPTY) != 0;
    }

<<<<<<< HEAD
    private boolean initialGroupsIsSet() {
      return (optBits & OPT_BIT_INITIAL_GROUPS) != 0;
    }

    private boolean parentIsSet() {
      return (initBits & INIT_BIT_PARENT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of AggregateTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!parentIsSet()) attributes.add("parent");
=======
    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PARENT) != 0) attributes.add("parent");
>>>>>>> main
      return "Cannot build AggregateTable, some of required attributes are not set " + attributes;
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
