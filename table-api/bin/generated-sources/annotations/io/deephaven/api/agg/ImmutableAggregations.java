package io.deephaven.api.agg;

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
 * Immutable implementation of {@link Aggregations}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggregations.builder()}.
 */
@Generated(from = "Aggregations", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggregations extends Aggregations {
  private final List<Aggregation> aggregations;

  private ImmutableAggregations(List<Aggregation> aggregations) {
    this.aggregations = aggregations;
  }

  /**
   * @return The value of the {@code aggregations} attribute
   */
  @Override
  public List<Aggregation> aggregations() {
    return aggregations;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Aggregations#aggregations() aggregations}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregations withAggregations(Aggregation... elements) {
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableAggregations(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link Aggregations#aggregations() aggregations}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of aggregations elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggregations withAggregations(Iterable<? extends Aggregation> elements) {
    if (this.aggregations == elements) return this;
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableAggregations(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggregations} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggregations
        && equalTo(0, (ImmutableAggregations) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggregations another) {
    return aggregations.equals(another.aggregations);
  }

  /**
   * Computes a hash code from attributes: {@code aggregations}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + aggregations.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Aggregations} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Aggregations{"
        + "aggregations=" + aggregations
        + "}";
  }

  private static ImmutableAggregations validate(ImmutableAggregations instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link Aggregations} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Aggregations instance
   */
  public static ImmutableAggregations copyOf(Aggregations instance) {
    if (instance instanceof ImmutableAggregations) {
      return (ImmutableAggregations) instance;
    }
    return ImmutableAggregations.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggregations ImmutableAggregations}.
   * <pre>
   * ImmutableAggregations.builder()
<<<<<<< HEAD
   *    .addAggregations|addAllAggregations(io.deephaven.api.agg.Aggregation) // {@link Aggregations#aggregations() aggregations} elements
=======
   *    .addAggregations|addAllAggregations(Aggregation) // {@link Aggregations#aggregations() aggregations} elements
>>>>>>> main
   *    .build();
   * </pre>
   * @return A new ImmutableAggregations builder
   */
  public static ImmutableAggregations.Builder builder() {
    return new ImmutableAggregations.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggregations ImmutableAggregations}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Aggregations", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements Aggregations.Builder {
    private List<Aggregation> aggregations = new ArrayList<Aggregation>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Aggregations} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Aggregations instance) {
      Objects.requireNonNull(instance, "instance");
      addAllAggregations(instance.aggregations());
      return this;
    }

    /**
     * Adds one element to {@link Aggregations#aggregations() aggregations} list.
     * @param element A aggregations element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAggregations(Aggregation element) {
      this.aggregations.add(Objects.requireNonNull(element, "aggregations element"));
      return this;
    }

    /**
     * Adds elements to {@link Aggregations#aggregations() aggregations} list.
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
     * Sets or replaces all elements for {@link Aggregations#aggregations() aggregations} list.
     * @param elements An iterable of aggregations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder aggregations(Iterable<? extends Aggregation> elements) {
      this.aggregations.clear();
      return addAllAggregations(elements);
    }

    /**
     * Adds elements to {@link Aggregations#aggregations() aggregations} list.
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
     * Builds a new {@link ImmutableAggregations ImmutableAggregations}.
     * @return An immutable instance of Aggregations
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggregations build() {
      return ImmutableAggregations.validate(new ImmutableAggregations(createUnmodifiableList(true, aggregations)));
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
