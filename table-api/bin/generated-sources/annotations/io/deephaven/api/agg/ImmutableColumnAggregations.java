package io.deephaven.api.agg;

import io.deephaven.api.Pair;
import io.deephaven.api.agg.spec.AggSpec;
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
 * Immutable implementation of {@link ColumnAggregations}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnAggregations.builder()}.
 */
@Generated(from = "ColumnAggregations", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableColumnAggregations extends ColumnAggregations {
  private final AggSpec spec;
  private final List<Pair> pairs;

  private ImmutableColumnAggregations(AggSpec spec, List<Pair> pairs) {
    this.spec = spec;
    this.pairs = pairs;
  }

  /**
   * @return The value of the {@code spec} attribute
   */
  @Override
  public AggSpec spec() {
    return spec;
  }

  /**
   * @return The value of the {@code pairs} attribute
   */
  @Override
  public List<Pair> pairs() {
    return pairs;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnAggregations#spec() spec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for spec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnAggregations withSpec(AggSpec value) {
    if (this.spec == value) return this;
    AggSpec newValue = Objects.requireNonNull(value, "spec");
    return validate(new ImmutableColumnAggregations(newValue, this.pairs));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnAggregations#pairs() pairs}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnAggregations withPairs(Pair... elements) {
    List<Pair> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableColumnAggregations(this.spec, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnAggregations#pairs() pairs}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of pairs elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnAggregations withPairs(Iterable<? extends Pair> elements) {
    if (this.pairs == elements) return this;
    List<Pair> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableColumnAggregations(this.spec, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnAggregations} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnAggregations
        && equalTo(0, (ImmutableColumnAggregations) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnAggregations another) {
    return spec.equals(another.spec)
        && pairs.equals(another.pairs);
  }

  /**
   * Computes a hash code from attributes: {@code spec}, {@code pairs}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + spec.hashCode();
    h += (h << 5) + pairs.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnAggregations} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnAggregations{"
        + "spec=" + spec
        + ", pairs=" + pairs
        + "}";
  }

  private static ImmutableColumnAggregations validate(ImmutableColumnAggregations instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ColumnAggregations} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ColumnAggregations instance
   */
  public static ImmutableColumnAggregations copyOf(ColumnAggregations instance) {
    if (instance instanceof ImmutableColumnAggregations) {
      return (ImmutableColumnAggregations) instance;
    }
    return ImmutableColumnAggregations.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnAggregations ImmutableColumnAggregations}.
   * <pre>
   * ImmutableColumnAggregations.builder()
   *    .spec(io.deephaven.api.agg.spec.AggSpec) // required {@link ColumnAggregations#spec() spec}
   *    .addPairs|addAllPairs(io.deephaven.api.Pair) // {@link ColumnAggregations#pairs() pairs} elements
   *    .build();
   * </pre>
   * @return A new ImmutableColumnAggregations builder
   */
  public static ImmutableColumnAggregations.Builder builder() {
    return new ImmutableColumnAggregations.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableColumnAggregations ImmutableColumnAggregations}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnAggregations", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ColumnAggregations.Builder {
    private static final long INIT_BIT_SPEC = 0x1L;
    private long initBits = 0x1L;

    private @Nullable AggSpec spec;
    private List<Pair> pairs = new ArrayList<Pair>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnAggregations} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ColumnAggregations instance) {
      Objects.requireNonNull(instance, "instance");
      spec(instance.spec());
      addAllPairs(instance.pairs());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnAggregations#spec() spec} attribute.
     * @param spec The value for spec 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder spec(AggSpec spec) {
      this.spec = Objects.requireNonNull(spec, "spec");
      initBits &= ~INIT_BIT_SPEC;
      return this;
    }

    /**
     * Adds one element to {@link ColumnAggregations#pairs() pairs} list.
     * @param element A pairs element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPairs(Pair element) {
      this.pairs.add(Objects.requireNonNull(element, "pairs element"));
      return this;
    }

    /**
     * Adds elements to {@link ColumnAggregations#pairs() pairs} list.
     * @param elements An array of pairs elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addPairs(Pair... elements) {
      for (Pair element : elements) {
        this.pairs.add(Objects.requireNonNull(element, "pairs element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ColumnAggregations#pairs() pairs} list.
     * @param elements An iterable of pairs elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder pairs(Iterable<? extends Pair> elements) {
      this.pairs.clear();
      return addAllPairs(elements);
    }

    /**
     * Adds elements to {@link ColumnAggregations#pairs() pairs} list.
     * @param elements An iterable of pairs elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllPairs(Iterable<? extends Pair> elements) {
      for (Pair element : elements) {
        this.pairs.add(Objects.requireNonNull(element, "pairs element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnAggregations ImmutableColumnAggregations}.
     * @return An immutable instance of ColumnAggregations
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnAggregations build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumnAggregations.validate(new ImmutableColumnAggregations(spec, createUnmodifiableList(true, pairs)));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SPEC) != 0) attributes.add("spec");
      return "Cannot build ColumnAggregations, some of required attributes are not set " + attributes;
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
