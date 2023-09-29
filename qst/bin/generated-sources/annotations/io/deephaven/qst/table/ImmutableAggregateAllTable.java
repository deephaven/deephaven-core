package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.spec.AggSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggregateAllTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggregateAllTable.builder()}.
 */
@Generated(from = "AggregateAllTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableAggregateAllTable extends AggregateAllTable {
  private transient final int depth;
  private final TableSpec parent;
  private final List<ColumnName> groupByColumns;
  private final AggSpec spec;

  private ImmutableAggregateAllTable(
      TableSpec parent,
      List<ColumnName> groupByColumns,
      AggSpec spec) {
    this.parent = parent;
    this.groupByColumns = groupByColumns;
    this.spec = spec;
    this.depth = super.depth();
  }

  /**
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
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
   * @return The value of the {@code spec} attribute
   */
  @Override
  public AggSpec spec() {
    return spec;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggregateAllTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggregateAllTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return new ImmutableAggregateAllTable(newValue, this.groupByColumns, this.spec);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggregateAllTable#groupByColumns() groupByColumns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateAllTable withGroupByColumns(ColumnName... elements) {
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableAggregateAllTable(this.parent, newValue, this.spec);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggregateAllTable#groupByColumns() groupByColumns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of groupByColumns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregateAllTable withGroupByColumns(Iterable<? extends ColumnName> elements) {
    if (this.groupByColumns == elements) return this;
    List<ColumnName> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableAggregateAllTable(this.parent, newValue, this.spec);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggregateAllTable#spec() spec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for spec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggregateAllTable withSpec(AggSpec value) {
    if (this.spec == value) return this;
    AggSpec newValue = Objects.requireNonNull(value, "spec");
    return new ImmutableAggregateAllTable(this.parent, this.groupByColumns, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggregateAllTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggregateAllTable
        && equalTo(0, (ImmutableAggregateAllTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggregateAllTable another) {
    return depth == another.depth
        && parent.equals(another.parent)
        && groupByColumns.equals(another.groupByColumns)
        && spec.equals(another.spec);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code parent}, {@code groupByColumns}, {@code spec}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + parent.hashCode();
    h += (h << 5) + groupByColumns.hashCode();
    h += (h << 5) + spec.hashCode();
    return h;
  }

  /**
   * Creates an immutable copy of a {@link AggregateAllTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggregateAllTable instance
   */
  public static ImmutableAggregateAllTable copyOf(AggregateAllTable instance) {
    if (instance instanceof ImmutableAggregateAllTable) {
      return (ImmutableAggregateAllTable) instance;
    }
    return ImmutableAggregateAllTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggregateAllTable ImmutableAggregateAllTable}.
   * <pre>
   * ImmutableAggregateAllTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link AggregateAllTable#parent() parent}
   *    .addGroupByColumns|addAllGroupByColumns(io.deephaven.api.ColumnName) // {@link AggregateAllTable#groupByColumns() groupByColumns} elements
   *    .spec(io.deephaven.api.agg.spec.AggSpec) // required {@link AggregateAllTable#spec() spec}
   *    .build();
   * </pre>
   * @return A new ImmutableAggregateAllTable builder
   */
  public static ImmutableAggregateAllTable.Builder builder() {
    return new ImmutableAggregateAllTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggregateAllTable ImmutableAggregateAllTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggregateAllTable", generator = "Immutables")
  public static final class Builder implements AggregateAllTable.Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private static final long INIT_BIT_SPEC = 0x2L;
    private long initBits = 0x3L;

    private TableSpec parent;
    private List<ColumnName> groupByColumns = new ArrayList<ColumnName>();
    private AggSpec spec;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.ByTableBase} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ByTableBase instance) {
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.AggregateAllTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggregateAllTable instance) {
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
      if (object instanceof SingleParentTable) {
        SingleParentTable instance = (SingleParentTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
      if (object instanceof AggregateAllTable) {
        AggregateAllTable instance = (AggregateAllTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          addAllGroupByColumns(instance.groupByColumns());
          bits |= 0x2L;
        }
        spec(instance.spec());
      }
    }

    /**
     * Initializes the value for the {@link AggregateAllTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
      return this;
    }

    /**
     * Adds one element to {@link AggregateAllTable#groupByColumns() groupByColumns} list.
     * @param element A groupByColumns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addGroupByColumns(ColumnName element) {
      this.groupByColumns.add(Objects.requireNonNull(element, "groupByColumns element"));
      return this;
    }

    /**
     * Adds elements to {@link AggregateAllTable#groupByColumns() groupByColumns} list.
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
     * Sets or replaces all elements for {@link AggregateAllTable#groupByColumns() groupByColumns} list.
     * @param elements An iterable of groupByColumns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder groupByColumns(Iterable<? extends ColumnName> elements) {
      this.groupByColumns.clear();
      return addAllGroupByColumns(elements);
    }

    /**
     * Adds elements to {@link AggregateAllTable#groupByColumns() groupByColumns} list.
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
     * Initializes the value for the {@link AggregateAllTable#spec() spec} attribute.
     * @param spec The value for spec 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder spec(AggSpec spec) {
      this.spec = Objects.requireNonNull(spec, "spec");
      initBits &= ~INIT_BIT_SPEC;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggregateAllTable ImmutableAggregateAllTable}.
     * @return An immutable instance of AggregateAllTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggregateAllTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableAggregateAllTable(parent, createUnmodifiableList(true, groupByColumns), spec);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PARENT) != 0) attributes.add("parent");
      if ((initBits & INIT_BIT_SPEC) != 0) attributes.add("spec");
      return "Cannot build AggregateAllTable, some of required attributes are not set " + attributes;
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
