package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
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
 * Immutable implementation of {@link NaturalJoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNaturalJoinTable.builder()}.
 */
@Generated(from = "NaturalJoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
<<<<<<< HEAD
final class ImmutableNaturalJoinTable extends NaturalJoinTable {
=======
public final class ImmutableNaturalJoinTable extends NaturalJoinTable {
>>>>>>> main
  private transient final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> matches;
  private final List<JoinAddition> additions;
<<<<<<< HEAD
  private transient final int hashCode;
=======
>>>>>>> main

  private ImmutableNaturalJoinTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> matches,
      List<JoinAddition> additions) {
    this.left = left;
    this.right = right;
    this.matches = matches;
    this.additions = additions;
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
   * @return The value of the {@code additions} attribute
   */
  @Override
  public List<JoinAddition> additions() {
    return additions;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link NaturalJoinTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableNaturalJoinTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableNaturalJoinTable(newValue, this.right, this.matches, this.additions));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link NaturalJoinTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableNaturalJoinTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableNaturalJoinTable(this.left, newValue, this.matches, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NaturalJoinTable#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNaturalJoinTable withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableNaturalJoinTable(this.left, this.right, newValue, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NaturalJoinTable#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNaturalJoinTable withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableNaturalJoinTable(this.left, this.right, newValue, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NaturalJoinTable#additions() additions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNaturalJoinTable withAdditions(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableNaturalJoinTable(this.left, this.right, this.matches, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link NaturalJoinTable#additions() additions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of additions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableNaturalJoinTable withAdditions(Iterable<? extends JoinAddition> elements) {
    if (this.additions == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableNaturalJoinTable(this.left, this.right, this.matches, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNaturalJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNaturalJoinTable
        && equalTo(0, (ImmutableNaturalJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableNaturalJoinTable another) {
<<<<<<< HEAD
    if (hashCode != another.hashCode) return false;
=======
>>>>>>> main
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && matches.equals(another.matches)
        && additions.equals(another.additions);
  }

  /**
<<<<<<< HEAD
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code additions}.
=======
   * Computes a hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code additions}.
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
    h += (h << 5) + additions.hashCode();
    return h;
  }

<<<<<<< HEAD
  private static final class InternerHolder {
    static final Map<ImmutableNaturalJoinTable, WeakReference<ImmutableNaturalJoinTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableNaturalJoinTable validate(ImmutableNaturalJoinTable instance) {
    instance.checkAdditions();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableNaturalJoinTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableNaturalJoinTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
=======
  private static ImmutableNaturalJoinTable validate(ImmutableNaturalJoinTable instance) {
    instance.checkAdditions();
    return instance;
>>>>>>> main
  }

  /**
   * Creates an immutable copy of a {@link NaturalJoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable NaturalJoinTable instance
   */
  public static ImmutableNaturalJoinTable copyOf(NaturalJoinTable instance) {
    if (instance instanceof ImmutableNaturalJoinTable) {
      return (ImmutableNaturalJoinTable) instance;
    }
    return ImmutableNaturalJoinTable.builder()
<<<<<<< HEAD
        .left(instance.left())
        .right(instance.right())
        .addAllMatches(instance.matches())
        .addAllAdditions(instance.additions())
=======
        .from(instance)
>>>>>>> main
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableNaturalJoinTable ImmutableNaturalJoinTable}.
   * <pre>
   * ImmutableNaturalJoinTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link NaturalJoinTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link NaturalJoinTable#right() right}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link NaturalJoinTable#matches() matches} elements
   *    .addAdditions|addAllAdditions(io.deephaven.api.JoinAddition) // {@link NaturalJoinTable#additions() additions} elements
   *    .build();
   * </pre>
   * @return A new ImmutableNaturalJoinTable builder
   */
  public static ImmutableNaturalJoinTable.Builder builder() {
    return new ImmutableNaturalJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNaturalJoinTable ImmutableNaturalJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NaturalJoinTable", generator = "Immutables")
  public static final class Builder implements NaturalJoinTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private long initBits = 0x3L;

    private TableSpec left;
    private TableSpec right;
<<<<<<< HEAD
    private final List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private final List<JoinAddition> additions = new ArrayList<JoinAddition>();
=======
    private List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private List<JoinAddition> additions = new ArrayList<JoinAddition>();
>>>>>>> main

    private Builder() {
    }

    /**
<<<<<<< HEAD
=======
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.NaturalJoinTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(NaturalJoinTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.Join} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Join instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.JoinBase} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinBase instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof NaturalJoinTable) {
        NaturalJoinTable instance = (NaturalJoinTable) object;
        if ((bits & 0x4L) == 0) {
          right(instance.right());
          bits |= 0x4L;
        }
        if ((bits & 0x1L) == 0) {
          addAllAdditions(instance.additions());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          left(instance.left());
          bits |= 0x2L;
        }
        if ((bits & 0x8L) == 0) {
          addAllMatches(instance.matches());
          bits |= 0x8L;
        }
      }
      if (object instanceof Join) {
        Join instance = (Join) object;
        if ((bits & 0x4L) == 0) {
          right(instance.right());
          bits |= 0x4L;
        }
        if ((bits & 0x1L) == 0) {
          addAllAdditions(instance.additions());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          left(instance.left());
          bits |= 0x2L;
        }
        if ((bits & 0x8L) == 0) {
          addAllMatches(instance.matches());
          bits |= 0x8L;
        }
      }
      if (object instanceof JoinBase) {
        JoinBase instance = (JoinBase) object;
        if ((bits & 0x4L) == 0) {
          right(instance.right());
          bits |= 0x4L;
        }
        if ((bits & 0x1L) == 0) {
          addAllAdditions(instance.additions());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          left(instance.left());
          bits |= 0x2L;
        }
        if ((bits & 0x8L) == 0) {
          addAllMatches(instance.matches());
          bits |= 0x8L;
        }
      }
    }

    /**
>>>>>>> main
     * Initializes the value for the {@link NaturalJoinTable#left() left} attribute.
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
     * Initializes the value for the {@link NaturalJoinTable#right() right} attribute.
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
     * Adds one element to {@link NaturalJoinTable#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link NaturalJoinTable#matches() matches} list.
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
     * Sets or replaces all elements for {@link NaturalJoinTable#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder matches(Iterable<? extends JoinMatch> elements) {
      this.matches.clear();
      return addAllMatches(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link NaturalJoinTable#matches() matches} list.
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
     * Adds one element to {@link NaturalJoinTable#additions() additions} list.
     * @param element A additions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAdditions(JoinAddition element) {
      this.additions.add(Objects.requireNonNull(element, "additions element"));
      return this;
    }

    /**
     * Adds elements to {@link NaturalJoinTable#additions() additions} list.
     * @param elements An array of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAdditions(JoinAddition... elements) {
      for (JoinAddition element : elements) {
        this.additions.add(Objects.requireNonNull(element, "additions element"));
      }
      return this;
    }


    /**
<<<<<<< HEAD
=======
     * Sets or replaces all elements for {@link NaturalJoinTable#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder additions(Iterable<? extends JoinAddition> elements) {
      this.additions.clear();
      return addAllAdditions(elements);
    }

    /**
>>>>>>> main
     * Adds elements to {@link NaturalJoinTable#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllAdditions(Iterable<? extends JoinAddition> elements) {
      for (JoinAddition element : elements) {
        this.additions.add(Objects.requireNonNull(element, "additions element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableNaturalJoinTable ImmutableNaturalJoinTable}.
     * @return An immutable instance of NaturalJoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNaturalJoinTable build() {
<<<<<<< HEAD
      checkRequiredAttributes();
      return ImmutableNaturalJoinTable.validate(new ImmutableNaturalJoinTable(left, right, createUnmodifiableList(true, matches), createUnmodifiableList(true, additions)));
    }

    private boolean leftIsSet() {
      return (initBits & INIT_BIT_LEFT) == 0;
    }

    private boolean rightIsSet() {
      return (initBits & INIT_BIT_RIGHT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of NaturalJoinTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
=======
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableNaturalJoinTable.validate(new ImmutableNaturalJoinTable(left, right, createUnmodifiableList(true, matches), createUnmodifiableList(true, additions)));
>>>>>>> main
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
<<<<<<< HEAD
      if (!leftIsSet()) attributes.add("left");
      if (!rightIsSet()) attributes.add("right");
=======
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
>>>>>>> main
      return "Cannot build NaturalJoinTable, some of required attributes are not set " + attributes;
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
