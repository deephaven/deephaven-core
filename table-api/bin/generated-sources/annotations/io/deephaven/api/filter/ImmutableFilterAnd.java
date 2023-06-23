package io.deephaven.api.filter;

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
 * Immutable implementation of {@link FilterAnd}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterAnd.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFilterAnd.of()}.
 */
@Generated(from = "FilterAnd", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterAnd extends FilterAnd {
  private final List<Filter> filters;

  private ImmutableFilterAnd(Iterable<? extends Filter> filters) {
    this.filters = createUnmodifiableList(false, createSafeList(filters, true, false));
  }

  private ImmutableFilterAnd(ImmutableFilterAnd original, List<Filter> filters) {
    this.filters = filters;
  }

  /**
   * The filters.
   * @return the filters
   */
  @Override
  public List<Filter> filters() {
    return filters;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FilterAnd#filters() filters}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterAnd withFilters(Filter... elements) {
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableFilterAnd(this, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FilterAnd#filters() filters}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of filters elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterAnd withFilters(Iterable<? extends Filter> elements) {
    if (this.filters == elements) return this;
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableFilterAnd(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterAnd} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterAnd
        && equalTo(0, (ImmutableFilterAnd) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterAnd another) {
    return filters.equals(another.filters);
  }

  /**
   * Computes a hash code from attributes: {@code filters}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + filters.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FilterAnd} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterAnd{"
        + "filters=" + filters
        + "}";
  }

  /**
   * Construct a new immutable {@code FilterAnd} instance.
   * @param filters The value for the {@code filters} attribute
   * @return An immutable FilterAnd instance
   */
  public static ImmutableFilterAnd of(List<Filter> filters) {
    return of((Iterable<? extends Filter>) filters);
  }

  /**
   * Construct a new immutable {@code FilterAnd} instance.
   * @param filters The value for the {@code filters} attribute
   * @return An immutable FilterAnd instance
   */
  public static ImmutableFilterAnd of(Iterable<? extends Filter> filters) {
    return validate(new ImmutableFilterAnd(filters));
  }

  private static ImmutableFilterAnd validate(ImmutableFilterAnd instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link FilterAnd} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterAnd instance
   */
  public static ImmutableFilterAnd copyOf(FilterAnd instance) {
    if (instance instanceof ImmutableFilterAnd) {
      return (ImmutableFilterAnd) instance;
    }
    return ImmutableFilterAnd.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterAnd ImmutableFilterAnd}.
   * <pre>
   * ImmutableFilterAnd.builder()
   *    .addFilters|addAllFilters(io.deephaven.api.filter.Filter) // {@link FilterAnd#filters() filters} elements
   *    .build();
   * </pre>
   * @return A new ImmutableFilterAnd builder
   */
  public static ImmutableFilterAnd.Builder builder() {
    return new ImmutableFilterAnd.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterAnd ImmutableFilterAnd}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterAnd", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterAnd.Builder {
    private List<Filter> filters = new ArrayList<Filter>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterAnd} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterAnd instance) {
      Objects.requireNonNull(instance, "instance");
      addAllFilters(instance.filters());
      return this;
    }

    /**
     * Adds one element to {@link FilterAnd#filters() filters} list.
     * @param element A filters element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFilters(Filter element) {
      this.filters.add(Objects.requireNonNull(element, "filters element"));
      return this;
    }

    /**
     * Adds elements to {@link FilterAnd#filters() filters} list.
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
     * Sets or replaces all elements for {@link FilterAnd#filters() filters} list.
     * @param elements An iterable of filters elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder filters(Iterable<? extends Filter> elements) {
      this.filters.clear();
      return addAllFilters(elements);
    }

    /**
     * Adds elements to {@link FilterAnd#filters() filters} list.
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
     * Builds a new {@link ImmutableFilterAnd ImmutableFilterAnd}.
     * @return An immutable instance of FilterAnd
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterAnd build() {
      return ImmutableFilterAnd.validate(new ImmutableFilterAnd(null, createUnmodifiableList(true, filters)));
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
