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
 * Immutable implementation of {@link FilterOr}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterOr.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFilterOr.of()}.
 */
@Generated(from = "FilterOr", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterOr extends FilterOr {
  private final List<Filter> filters;

  private ImmutableFilterOr(Iterable<? extends Filter> filters) {
    this.filters = createUnmodifiableList(false, createSafeList(filters, true, false));
  }

  private ImmutableFilterOr(ImmutableFilterOr original, List<Filter> filters) {
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
   * Copy the current immutable object with elements that replace the content of {@link FilterOr#filters() filters}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterOr withFilters(Filter... elements) {
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableFilterOr(this, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link FilterOr#filters() filters}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of filters elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableFilterOr withFilters(Iterable<? extends Filter> elements) {
    if (this.filters == elements) return this;
    List<Filter> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableFilterOr(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterOr} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterOr
        && equalTo(0, (ImmutableFilterOr) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterOr another) {
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
   * Prints the immutable value {@code FilterOr} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterOr{"
        + "filters=" + filters
        + "}";
  }

  /**
   * Construct a new immutable {@code FilterOr} instance.
   * @param filters The value for the {@code filters} attribute
   * @return An immutable FilterOr instance
   */
  public static ImmutableFilterOr of(List<Filter> filters) {
    return of((Iterable<? extends Filter>) filters);
  }

  /**
   * Construct a new immutable {@code FilterOr} instance.
   * @param filters The value for the {@code filters} attribute
   * @return An immutable FilterOr instance
   */
  public static ImmutableFilterOr of(Iterable<? extends Filter> filters) {
    return validate(new ImmutableFilterOr(filters));
  }

  private static ImmutableFilterOr validate(ImmutableFilterOr instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link FilterOr} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterOr instance
   */
  public static ImmutableFilterOr copyOf(FilterOr instance) {
    if (instance instanceof ImmutableFilterOr) {
      return (ImmutableFilterOr) instance;
    }
    return ImmutableFilterOr.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterOr ImmutableFilterOr}.
   * <pre>
   * ImmutableFilterOr.builder()
<<<<<<< HEAD
   *    .addFilters|addAllFilters(io.deephaven.api.filter.Filter) // {@link FilterOr#filters() filters} elements
=======
   *    .addFilters|addAllFilters(Filter) // {@link FilterOr#filters() filters} elements
>>>>>>> main
   *    .build();
   * </pre>
   * @return A new ImmutableFilterOr builder
   */
  public static ImmutableFilterOr.Builder builder() {
    return new ImmutableFilterOr.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterOr ImmutableFilterOr}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterOr", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements FilterOr.Builder {
    private List<Filter> filters = new ArrayList<Filter>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterOr} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterOr instance) {
      Objects.requireNonNull(instance, "instance");
      addAllFilters(instance.filters());
      return this;
    }

    /**
     * Adds one element to {@link FilterOr#filters() filters} list.
     * @param element A filters element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFilters(Filter element) {
      this.filters.add(Objects.requireNonNull(element, "filters element"));
      return this;
    }

    /**
     * Adds elements to {@link FilterOr#filters() filters} list.
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
     * Sets or replaces all elements for {@link FilterOr#filters() filters} list.
     * @param elements An iterable of filters elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder filters(Iterable<? extends Filter> elements) {
      this.filters.clear();
      return addAllFilters(elements);
    }

    /**
     * Adds elements to {@link FilterOr#filters() filters} list.
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
     * Builds a new {@link ImmutableFilterOr ImmutableFilterOr}.
     * @return An immutable instance of FilterOr
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterOr build() {
      return ImmutableFilterOr.validate(new ImmutableFilterOr(null, createUnmodifiableList(true, filters)));
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
