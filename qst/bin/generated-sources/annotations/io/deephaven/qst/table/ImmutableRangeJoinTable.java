package io.deephaven.qst.table;

import io.deephaven.api.JoinMatch;
import io.deephaven.api.RangeJoinMatch;
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
import java.util.WeakHashMap;
=======
import java.util.Objects;
>>>>>>> main
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link RangeJoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRangeJoinTable.builder()}.
 */
@Generated(from = "RangeJoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableRangeJoinTable extends RangeJoinTable {
=======
public final class ImmutableRangeJoinTable extends RangeJoinTable {
>>>>>>> main
  private transient final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> exactMatches;
  private final RangeJoinMatch rangeMatch;
  private final List<Aggregation> aggregations;
<<<<<<< HEAD
  private transient final int hashCode;
=======
>>>>>>> main

  private ImmutableRangeJoinTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> exactMatches,
      RangeJoinMatch rangeMatch,
      List<Aggregation> aggregations) {
    this.left = left;
    this.right = right;
    this.exactMatches = exactMatches;
    this.rangeMatch = rangeMatch;
    this.aggregations = aggregations;
    this.depth = super.depth();
<<<<<<< HEAD
    this.hashCode = computeHashCode();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
=======
  }

  /**
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
>>>>>>> main
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code left} attribute
   */
  @Override
  public TableSpec left() {
    return left;
  }

  /**
   * @return The value of the {@code right} attribute
   */
  @Override
  public TableSpec right() {
    return right;
  }

  /**
   * @return The value of the {@code exactMatches} attribute
   */
  @Override
  public List<JoinMatch> exactMatches() {
    return exactMatches;
  }

  /**
   * @return The value of the {@code rangeMatch} attribute
   */
  @Override
  public RangeJoinMatch rangeMatch() {
    return rangeMatch;
  }

  /**
   * @return The value of the {@code aggregations} attribute
   */
  @Override
  public List<Aggregation> aggregations() {
    return aggregations;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableRangeJoinTable(newValue, this.right, this.exactMatches, this.rangeMatch, this.aggregations));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableRangeJoinTable(this.left, newValue, this.exactMatches, this.rangeMatch, this.aggregations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link RangeJoinTable#exactMatches() exactMatches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableRangeJoinTable withExactMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableRangeJoinTable(this.left, this.right, newValue, this.rangeMatch, this.aggregations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link RangeJoinTable#exactMatches() exactMatches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of exactMatches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableRangeJoinTable withExactMatches(Iterable<? extends JoinMatch> elements) {
    if (this.exactMatches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableRangeJoinTable(this.left, this.right, newValue, this.rangeMatch, this.aggregations));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RangeJoinTable#rangeMatch() rangeMatch} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rangeMatch
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRangeJoinTable withRangeMatch(RangeJoinMatch value) {
    if (this.rangeMatch == value) return this;
    RangeJoinMatch newValue = Objects.requireNonNull(value, "rangeMatch");
    return validate(new ImmutableRangeJoinTable(this.left, this.right, this.exactMatches, newValue, this.aggregations));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link RangeJoinTable#aggregations() aggregations}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableRangeJoinTable withAggregations(Aggregation... elements) {
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableRangeJoinTable(this.left, this.right, this.exactMatches, this.rangeMatch, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link RangeJoinTable#aggregations() aggregations}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of aggregations elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableRangeJoinTable withAggregations(Iterable<? extends Aggregation> elements) {
    if (this.aggregations == elements) return this;
    List<Aggregation> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableRangeJoinTable(this.left, this.right, this.exactMatches, this.rangeMatch, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRangeJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRangeJoinTable
        && equalTo(0, (ImmutableRangeJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableRangeJoinTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
=======
>>>>>>> main
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && exactMatches.equals(another.exactMatches)
        && rangeMatch.equals(another.rangeMatch)
        && aggregations.equals(another.aggregations);
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code exactMatches}, {@code rangeMatch}, {@code aggregations}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code exactMatches}, {@code rangeMatch}, {@code aggregations}.
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
=======
    int h = 5381;
>>>>>>> main
    h += (h << 5) + depth;
    h += (h << 5) + left.hashCode();
    h += (h << 5) + right.hashCode();
    h += (h << 5) + exactMatches.hashCode();
    h += (h << 5) + rangeMatch.hashCode();
    h += (h << 5) + aggregations.hashCode();
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableRangeJoinTable, WeakReference<ImmutableRangeJoinTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableRangeJoinTable validate(ImmutableRangeJoinTable instance) {
    instance.checkAggregationsNonEmpty();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableRangeJoinTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableRangeJoinTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
=======
  private static ImmutableRangeJoinTable validate(ImmutableRangeJoinTable instance) {
    instance.checkAggregationsNonEmpty();
    return instance;
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link RangeJoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RangeJoinTable instance
   */
  public static ImmutableRangeJoinTable copyOf(RangeJoinTable instance) {
    if (instance instanceof ImmutableRangeJoinTable) {
      return (ImmutableRangeJoinTable) instance;
    }
    return ImmutableRangeJoinTable.builder()
<<<<<<< HEAD
        .left(instance.left())
        .right(instance.right())
        .addAllExactMatches(instance.exactMatches())
        .rangeMatch(instance.rangeMatch())
        .addAllAggregations(instance.aggregations())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRangeJoinTable ImmutableRangeJoinTable}.
   * <pre>
   * ImmutableRangeJoinTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link RangeJoinTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link RangeJoinTable#right() right}
   *    .addExactMatches|addAllExactMatches(io.deephaven.api.JoinMatch) // {@link RangeJoinTable#exactMatches() exactMatches} elements
   *    .rangeMatch(io.deephaven.api.RangeJoinMatch) // required {@link RangeJoinTable#rangeMatch() rangeMatch}
   *    .addAggregations|addAllAggregations(io.deephaven.api.agg.Aggregation) // {@link RangeJoinTable#aggregations() aggregations} elements
   *    .build();
   * </pre>
   * @return A new ImmutableRangeJoinTable builder
   */
  public static ImmutableRangeJoinTable.Builder builder() {
    return new ImmutableRangeJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRangeJoinTable ImmutableRangeJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RangeJoinTable", generator = "Immutables")
  public static final class Builder implements RangeJoinTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private static final long INIT_BIT_RANGE_MATCH = 0x4L;
    private long initBits = 0x7L;

    private TableSpec left;
    private TableSpec right;
<<<<<<< HEAD
    private final List<JoinMatch> exactMatches = new ArrayList<JoinMatch>();
    private RangeJoinMatch rangeMatch;
    private final List<Aggregation> aggregations = new ArrayList<Aggregation>();
=======
    private List<JoinMatch> exactMatches = new ArrayList<JoinMatch>();
    private RangeJoinMatch rangeMatch;
    private List<Aggregation> aggregations = new ArrayList<Aggregation>();
>>>>>>> main

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code RangeJoinTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RangeJoinTable instance) {
      Objects.requireNonNull(instance, "instance");
      left(instance.left());
      right(instance.right());
      addAllExactMatches(instance.exactMatches());
      rangeMatch(instance.rangeMatch());
      addAllAggregations(instance.aggregations());
      return this;
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link RangeJoinTable#left() left} attribute.
     * @param left The value for left 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder left(TableSpec left) {
<<<<<<< HEAD
      checkNotIsSet(leftIsSet(), "left");
=======
>>>>>>> main
      this.left = Objects.requireNonNull(left, "left");
      initBits &= ~INIT_BIT_LEFT;
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinTable#right() right} attribute.
     * @param right The value for right 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder right(TableSpec right) {
<<<<<<< HEAD
      checkNotIsSet(rightIsSet(), "right");
=======
>>>>>>> main
      this.right = Objects.requireNonNull(right, "right");
      initBits &= ~INIT_BIT_RIGHT;
      return this;
    }

    /**
     * Adds one element to {@link RangeJoinTable#exactMatches() exactMatches} list.
     * @param element A exactMatches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addExactMatches(JoinMatch element) {
      this.exactMatches.add(Objects.requireNonNull(element, "exactMatches element"));
      return this;
    }

    /**
     * Adds elements to {@link RangeJoinTable#exactMatches() exactMatches} list.
     * @param elements An array of exactMatches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addExactMatches(JoinMatch... elements) {
      for (JoinMatch element : elements) {
        this.exactMatches.add(Objects.requireNonNull(element, "exactMatches element"));
      }
      return this;
    }


    /**
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link RangeJoinTable#exactMatches() exactMatches} list.
     * @param elements An iterable of exactMatches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder exactMatches(Iterable<? extends JoinMatch> elements) {
      this.exactMatches.clear();
      return addAllExactMatches(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link RangeJoinTable#exactMatches() exactMatches} list.
     * @param elements An iterable of exactMatches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllExactMatches(Iterable<? extends JoinMatch> elements) {
      for (JoinMatch element : elements) {
        this.exactMatches.add(Objects.requireNonNull(element, "exactMatches element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link RangeJoinTable#rangeMatch() rangeMatch} attribute.
     * @param rangeMatch The value for rangeMatch 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rangeMatch(RangeJoinMatch rangeMatch) {
<<<<<<< HEAD
      checkNotIsSet(rangeMatchIsSet(), "rangeMatch");
=======
>>>>>>> main
      this.rangeMatch = Objects.requireNonNull(rangeMatch, "rangeMatch");
      initBits &= ~INIT_BIT_RANGE_MATCH;
      return this;
    }

    /**
     * Adds one element to {@link RangeJoinTable#aggregations() aggregations} list.
     * @param element A aggregations element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAggregations(Aggregation element) {
      this.aggregations.add(Objects.requireNonNull(element, "aggregations element"));
      return this;
    }

    /**
     * Adds elements to {@link RangeJoinTable#aggregations() aggregations} list.
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
     * Sets or replaces all elements for {@link RangeJoinTable#aggregations() aggregations} list.
     * @param elements An iterable of aggregations elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder aggregations(Iterable<? extends Aggregation> elements) {
      this.aggregations.clear();
      return addAllAggregations(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link RangeJoinTable#aggregations() aggregations} list.
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
     * Builds a new {@link ImmutableRangeJoinTable ImmutableRangeJoinTable}.
     * @return An immutable instance of RangeJoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRangeJoinTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
>>>>>>> main
      return ImmutableRangeJoinTable.validate(new ImmutableRangeJoinTable(
          left,
          right,
          createUnmodifiableList(true, exactMatches),
          rangeMatch,
          createUnmodifiableList(true, aggregations)));
    }

<<<<<<< HEAD
    private boolean leftIsSet() {
      return (initBits & INIT_BIT_LEFT) == 0;
    }

    private boolean rightIsSet() {
      return (initBits & INIT_BIT_RIGHT) == 0;
    }

    private boolean rangeMatchIsSet() {
      return (initBits & INIT_BIT_RANGE_MATCH) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of RangeJoinTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!leftIsSet()) attributes.add("left");
      if (!rightIsSet()) attributes.add("right");
      if (!rangeMatchIsSet()) attributes.add("rangeMatch");
=======
    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
      if ((initBits & INIT_BIT_RANGE_MATCH) != 0) attributes.add("rangeMatch");
>>>>>>> main
      return "Cannot build RangeJoinTable, some of required attributes are not set " + attributes;
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
