package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MergeTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMergeTable.builder()}.
 */
@Generated(from = "MergeTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableMergeTable extends MergeTable {
  private transient final int depth;
  private final List<TableSpec> tables;

  private ImmutableMergeTable(List<TableSpec> tables) {
    this.tables = tables;
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
   * @return The value of the {@code tables} attribute
   */
  @Override
  public List<TableSpec> tables() {
    return tables;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MergeTable#tables() tables}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMergeTable withTables(TableSpec... elements) {
    List<TableSpec> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableMergeTable(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MergeTable#tables() tables}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of tables elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMergeTable withTables(Iterable<? extends TableSpec> elements) {
    if (this.tables == elements) return this;
    List<TableSpec> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableMergeTable(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMergeTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMergeTable
        && equalTo(0, (ImmutableMergeTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableMergeTable another) {
    return depth == another.depth
        && tables.equals(another.tables);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code tables}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + tables.hashCode();
    return h;
  }

  private static ImmutableMergeTable validate(ImmutableMergeTable instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link MergeTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable MergeTable instance
   */
  public static ImmutableMergeTable copyOf(MergeTable instance) {
    if (instance instanceof ImmutableMergeTable) {
      return (ImmutableMergeTable) instance;
    }
    return ImmutableMergeTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableMergeTable ImmutableMergeTable}.
   * <pre>
   * ImmutableMergeTable.builder()
   *    .addTables|addAllTables(io.deephaven.qst.table.TableSpec) // {@link MergeTable#tables() tables} elements
   *    .build();
   * </pre>
   * @return A new ImmutableMergeTable builder
   */
  public static ImmutableMergeTable.Builder builder() {
    return new ImmutableMergeTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMergeTable ImmutableMergeTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MergeTable", generator = "Immutables")
  public static final class Builder implements MergeTable.Builder {
    private List<TableSpec> tables = new ArrayList<TableSpec>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code MergeTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(MergeTable instance) {
      Objects.requireNonNull(instance, "instance");
      addAllTables(instance.tables());
      return this;
    }

    /**
     * Adds one element to {@link MergeTable#tables() tables} list.
     * @param element A tables element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addTables(TableSpec element) {
      this.tables.add(Objects.requireNonNull(element, "tables element"));
      return this;
    }

    /**
     * Adds elements to {@link MergeTable#tables() tables} list.
     * @param elements An array of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addTables(TableSpec... elements) {
      for (TableSpec element : elements) {
        this.tables.add(Objects.requireNonNull(element, "tables element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link MergeTable#tables() tables} list.
     * @param elements An iterable of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder tables(Iterable<? extends TableSpec> elements) {
      this.tables.clear();
      return addAllTables(elements);
    }

    /**
     * Adds elements to {@link MergeTable#tables() tables} list.
     * @param elements An iterable of tables elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllTables(Iterable<? extends TableSpec> elements) {
      for (TableSpec element : elements) {
        this.tables.add(Objects.requireNonNull(element, "tables element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableMergeTable ImmutableMergeTable}.
     * @return An immutable instance of MergeTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMergeTable build() {
      return ImmutableMergeTable.validate(new ImmutableMergeTable(createUnmodifiableList(true, tables)));
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
