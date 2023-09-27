package io.deephaven.qst.table;

import io.deephaven.api.JoinMatch;
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
 * Immutable implementation of {@link WhereInTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableWhereInTable.builder()}.
 */
@Generated(from = "WhereInTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableWhereInTable extends WhereInTable {
=======
public final class ImmutableWhereInTable extends WhereInTable {
>>>>>>> main
  private transient final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> matches;
  private final boolean inverted;
<<<<<<< HEAD
  private transient final int hashCode;
=======
>>>>>>> main

  private ImmutableWhereInTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> matches,
      boolean inverted) {
    this.left = left;
    this.right = right;
    this.matches = matches;
    this.inverted = inverted;
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
   * @return The value of the {@code matches} attribute
   */
  @Override
  public List<JoinMatch> matches() {
    return matches;
  }

  /**
   * @return The value of the {@code inverted} attribute
   */
  @Override
  public boolean inverted() {
    return inverted;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WhereInTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWhereInTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableWhereInTable(newValue, this.right, this.matches, this.inverted));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WhereInTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWhereInTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableWhereInTable(this.left, newValue, this.matches, this.inverted));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link WhereInTable#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableWhereInTable withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableWhereInTable(this.left, this.right, newValue, this.inverted));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link WhereInTable#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableWhereInTable withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableWhereInTable(this.left, this.right, newValue, this.inverted));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WhereInTable#inverted() inverted} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for inverted
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWhereInTable withInverted(boolean value) {
    if (this.inverted == value) return this;
    return validate(new ImmutableWhereInTable(this.left, this.right, this.matches, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableWhereInTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableWhereInTable
        && equalTo(0, (ImmutableWhereInTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableWhereInTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
=======
>>>>>>> main
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && matches.equals(another.matches)
        && inverted == another.inverted;
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code inverted}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code inverted}.
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
    h += (h << 5) + matches.hashCode();
    h += (h << 5) + Boolean.hashCode(inverted);
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableWhereInTable, WeakReference<ImmutableWhereInTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableWhereInTable validate(ImmutableWhereInTable instance) {
    instance.checkNotEmpty();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableWhereInTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableWhereInTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
=======
  private static ImmutableWhereInTable validate(ImmutableWhereInTable instance) {
    instance.checkNotEmpty();
    return instance;
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link WhereInTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable WhereInTable instance
   */
  public static ImmutableWhereInTable copyOf(WhereInTable instance) {
    if (instance instanceof ImmutableWhereInTable) {
      return (ImmutableWhereInTable) instance;
    }
    return ImmutableWhereInTable.builder()
<<<<<<< HEAD
        .left(instance.left())
        .right(instance.right())
        .addAllMatches(instance.matches())
        .inverted(instance.inverted())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableWhereInTable ImmutableWhereInTable}.
   * <pre>
   * ImmutableWhereInTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link WhereInTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link WhereInTable#right() right}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link WhereInTable#matches() matches} elements
   *    .inverted(boolean) // required {@link WhereInTable#inverted() inverted}
   *    .build();
   * </pre>
   * @return A new ImmutableWhereInTable builder
   */
  public static ImmutableWhereInTable.Builder builder() {
    return new ImmutableWhereInTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableWhereInTable ImmutableWhereInTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "WhereInTable", generator = "Immutables")
  public static final class Builder implements WhereInTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private static final long INIT_BIT_INVERTED = 0x4L;
    private long initBits = 0x7L;

    private TableSpec left;
    private TableSpec right;
<<<<<<< HEAD
    private final List<JoinMatch> matches = new ArrayList<JoinMatch>();
=======
    private List<JoinMatch> matches = new ArrayList<JoinMatch>();
>>>>>>> main
    private boolean inverted;

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code WhereInTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(WhereInTable instance) {
      Objects.requireNonNull(instance, "instance");
      left(instance.left());
      right(instance.right());
      addAllMatches(instance.matches());
      inverted(instance.inverted());
      return this;
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link WhereInTable#left() left} attribute.
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
     * Initializes the value for the {@link WhereInTable#right() right} attribute.
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
     * Adds one element to {@link WhereInTable#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link WhereInTable#matches() matches} list.
     * @param elements An array of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch... elements) {
      for (JoinMatch element : elements) {
        this.matches.add(Objects.requireNonNull(element, "matches element"));
      }
      return this;
    }


    /**
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link WhereInTable#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder matches(Iterable<? extends JoinMatch> elements) {
      this.matches.clear();
      return addAllMatches(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link WhereInTable#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllMatches(Iterable<? extends JoinMatch> elements) {
      for (JoinMatch element : elements) {
        this.matches.add(Objects.requireNonNull(element, "matches element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link WhereInTable#inverted() inverted} attribute.
     * @param inverted The value for inverted 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder inverted(boolean inverted) {
<<<<<<< HEAD
      checkNotIsSet(invertedIsSet(), "inverted");
=======
>>>>>>> main
      this.inverted = inverted;
      initBits &= ~INIT_BIT_INVERTED;
      return this;
    }

    /**
     * Builds a new {@link ImmutableWhereInTable ImmutableWhereInTable}.
     * @return An immutable instance of WhereInTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableWhereInTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
      return ImmutableWhereInTable.validate(new ImmutableWhereInTable(left, right, createUnmodifiableList(true, matches), inverted));
    }

    private boolean leftIsSet() {
      return (initBits & INIT_BIT_LEFT) == 0;
    }

    private boolean rightIsSet() {
      return (initBits & INIT_BIT_RIGHT) == 0;
    }

    private boolean invertedIsSet() {
      return (initBits & INIT_BIT_INVERTED) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of WhereInTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableWhereInTable.validate(new ImmutableWhereInTable(left, right, createUnmodifiableList(true, matches), inverted));
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
<<<<<<< HEAD
      if (!leftIsSet()) attributes.add("left");
      if (!rightIsSet()) attributes.add("right");
      if (!invertedIsSet()) attributes.add("inverted");
=======
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
      if ((initBits & INIT_BIT_INVERTED) != 0) attributes.add("inverted");
>>>>>>> main
      return "Cannot build WhereInTable, some of required attributes are not set " + attributes;
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
