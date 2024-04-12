package io.deephaven.api.agg.spec;

import io.deephaven.api.SortColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecSortedLast}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecSortedLast.builder()}.
 */
@Generated(from = "AggSpecSortedLast", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecSortedLast extends AggSpecSortedLast {
  private final List<SortColumn> columns;

  private ImmutableAggSpecSortedLast(List<SortColumn> columns) {
    this.columns = columns;
  }

  /**
   * The columns to sort on to determine the order within each group.
   * @return The sort columns
   */
  @Override
  public List<SortColumn> columns() {
    return columns;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggSpecSortedLast#columns() columns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecSortedLast withColumns(SortColumn... elements) {
    List<SortColumn> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableAggSpecSortedLast(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AggSpecSortedLast#columns() columns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecSortedLast withColumns(Iterable<? extends SortColumn> elements) {
    if (this.columns == elements) return this;
    List<SortColumn> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableAggSpecSortedLast(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecSortedLast} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecSortedLast
        && equalTo(0, (ImmutableAggSpecSortedLast) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecSortedLast another) {
    return columns.equals(another.columns);
  }

  /**
   * Computes a hash code from attributes: {@code columns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + columns.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecSortedLast} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecSortedLast{"
        + "columns=" + columns
        + "}";
  }

  private static ImmutableAggSpecSortedLast validate(ImmutableAggSpecSortedLast instance) {
    instance.checkSortOrder();
    instance.nonEmptyColumns();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AggSpecSortedLast} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecSortedLast instance
   */
  public static ImmutableAggSpecSortedLast copyOf(AggSpecSortedLast instance) {
    if (instance instanceof ImmutableAggSpecSortedLast) {
      return (ImmutableAggSpecSortedLast) instance;
    }
    return ImmutableAggSpecSortedLast.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecSortedLast ImmutableAggSpecSortedLast}.
   * <pre>
   * ImmutableAggSpecSortedLast.builder()
   *    .addColumns|addAllColumns(io.deephaven.api.SortColumn) // {@link AggSpecSortedLast#columns() columns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecSortedLast builder
   */
  public static ImmutableAggSpecSortedLast.Builder builder() {
    return new ImmutableAggSpecSortedLast.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecSortedLast ImmutableAggSpecSortedLast}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecSortedLast", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements AggSpecSortedLast.Builder {
    private List<SortColumn> columns = new ArrayList<SortColumn>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecSortedLast} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecSortedLast instance) {
      Objects.requireNonNull(instance, "instance");
      addAllColumns(instance.columns());
      return this;
    }

    /**
     * Adds one element to {@link AggSpecSortedLast#columns() columns} list.
     * @param element A columns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(SortColumn element) {
      this.columns.add(Objects.requireNonNull(element, "columns element"));
      return this;
    }

    /**
     * Adds elements to {@link AggSpecSortedLast#columns() columns} list.
     * @param elements An array of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(SortColumn... elements) {
      for (SortColumn element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link AggSpecSortedLast#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder columns(Iterable<? extends SortColumn> elements) {
      this.columns.clear();
      return addAllColumns(elements);
    }

    /**
     * Adds elements to {@link AggSpecSortedLast#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllColumns(Iterable<? extends SortColumn> elements) {
      for (SortColumn element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecSortedLast ImmutableAggSpecSortedLast}.
     * @return An immutable instance of AggSpecSortedLast
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecSortedLast build() {
      return ImmutableAggSpecSortedLast.validate(new ImmutableAggSpecSortedLast(createUnmodifiableList(true, columns)));
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
