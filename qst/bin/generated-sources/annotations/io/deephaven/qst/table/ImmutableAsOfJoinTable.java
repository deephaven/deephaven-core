package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinMatch;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AsOfJoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAsOfJoinTable.builder()}.
 */
@Generated(from = "AsOfJoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableAsOfJoinTable extends AsOfJoinTable {
  private transient final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> matches;
  private final List<JoinAddition> additions;
  private final AsOfJoinMatch joinMatch;

  private ImmutableAsOfJoinTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> matches,
      List<JoinAddition> additions,
      AsOfJoinMatch joinMatch) {
    this.left = left;
    this.right = right;
    this.matches = matches;
    this.additions = additions;
    this.joinMatch = joinMatch;
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
   * @return The value of the {@code joinMatch} attribute
   */
  @Override
  public AsOfJoinMatch joinMatch() {
    return joinMatch;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableAsOfJoinTable(newValue, this.right, this.matches, this.additions, this.joinMatch));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableAsOfJoinTable(this.left, newValue, this.matches, this.additions, this.joinMatch));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AsOfJoinTable#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAsOfJoinTable withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableAsOfJoinTable(this.left, this.right, newValue, this.additions, this.joinMatch));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AsOfJoinTable#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAsOfJoinTable withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableAsOfJoinTable(this.left, this.right, newValue, this.additions, this.joinMatch));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AsOfJoinTable#additions() additions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAsOfJoinTable withAdditions(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableAsOfJoinTable(this.left, this.right, this.matches, newValue, this.joinMatch));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AsOfJoinTable#additions() additions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of additions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAsOfJoinTable withAdditions(Iterable<? extends JoinAddition> elements) {
    if (this.additions == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableAsOfJoinTable(this.left, this.right, this.matches, newValue, this.joinMatch));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AsOfJoinTable#joinMatch() joinMatch} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for joinMatch
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAsOfJoinTable withJoinMatch(AsOfJoinMatch value) {
    if (this.joinMatch == value) return this;
    AsOfJoinMatch newValue = Objects.requireNonNull(value, "joinMatch");
    return validate(new ImmutableAsOfJoinTable(this.left, this.right, this.matches, this.additions, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAsOfJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAsOfJoinTable
        && equalTo(0, (ImmutableAsOfJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableAsOfJoinTable another) {
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && matches.equals(another.matches)
        && additions.equals(another.additions)
        && joinMatch.equals(another.joinMatch);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code additions}, {@code joinMatch}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + left.hashCode();
    h += (h << 5) + right.hashCode();
    h += (h << 5) + matches.hashCode();
    h += (h << 5) + additions.hashCode();
    h += (h << 5) + joinMatch.hashCode();
    return h;
  }

  private static ImmutableAsOfJoinTable validate(ImmutableAsOfJoinTable instance) {
    instance.checkAdditions();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AsOfJoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AsOfJoinTable instance
   */
  public static ImmutableAsOfJoinTable copyOf(AsOfJoinTable instance) {
    if (instance instanceof ImmutableAsOfJoinTable) {
      return (ImmutableAsOfJoinTable) instance;
    }
    return ImmutableAsOfJoinTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAsOfJoinTable ImmutableAsOfJoinTable}.
   * <pre>
   * ImmutableAsOfJoinTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link AsOfJoinTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link AsOfJoinTable#right() right}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link AsOfJoinTable#matches() matches} elements
   *    .addAdditions|addAllAdditions(io.deephaven.api.JoinAddition) // {@link AsOfJoinTable#additions() additions} elements
   *    .joinMatch(io.deephaven.api.AsOfJoinMatch) // required {@link AsOfJoinTable#joinMatch() joinMatch}
   *    .build();
   * </pre>
   * @return A new ImmutableAsOfJoinTable builder
   */
  public static ImmutableAsOfJoinTable.Builder builder() {
    return new ImmutableAsOfJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAsOfJoinTable ImmutableAsOfJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AsOfJoinTable", generator = "Immutables")
  public static final class Builder implements AsOfJoinTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private static final long INIT_BIT_JOIN_MATCH = 0x4L;
    private long initBits = 0x7L;

    private TableSpec left;
    private TableSpec right;
    private List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private List<JoinAddition> additions = new ArrayList<JoinAddition>();
    private AsOfJoinMatch joinMatch;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.AsOfJoinTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AsOfJoinTable instance) {
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
      if (object instanceof AsOfJoinTable) {
        AsOfJoinTable instance = (AsOfJoinTable) object;
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
        joinMatch(instance.joinMatch());
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
     * Initializes the value for the {@link AsOfJoinTable#left() left} attribute.
     * @param left The value for left 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder left(TableSpec left) {
      this.left = Objects.requireNonNull(left, "left");
      initBits &= ~INIT_BIT_LEFT;
      return this;
    }

    /**
     * Initializes the value for the {@link AsOfJoinTable#right() right} attribute.
     * @param right The value for right 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder right(TableSpec right) {
      this.right = Objects.requireNonNull(right, "right");
      initBits &= ~INIT_BIT_RIGHT;
      return this;
    }

    /**
     * Adds one element to {@link AsOfJoinTable#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link AsOfJoinTable#matches() matches} list.
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
     * Sets or replaces all elements for {@link AsOfJoinTable#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder matches(Iterable<? extends JoinMatch> elements) {
      this.matches.clear();
      return addAllMatches(elements);
    }

    /**
     * Adds elements to {@link AsOfJoinTable#matches() matches} list.
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
     * Adds one element to {@link AsOfJoinTable#additions() additions} list.
     * @param element A additions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAdditions(JoinAddition element) {
      this.additions.add(Objects.requireNonNull(element, "additions element"));
      return this;
    }

    /**
     * Adds elements to {@link AsOfJoinTable#additions() additions} list.
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
     * Sets or replaces all elements for {@link AsOfJoinTable#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder additions(Iterable<? extends JoinAddition> elements) {
      this.additions.clear();
      return addAllAdditions(elements);
    }

    /**
     * Adds elements to {@link AsOfJoinTable#additions() additions} list.
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
     * Initializes the value for the {@link AsOfJoinTable#joinMatch() joinMatch} attribute.
     * @param joinMatch The value for joinMatch 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder joinMatch(AsOfJoinMatch joinMatch) {
      this.joinMatch = Objects.requireNonNull(joinMatch, "joinMatch");
      initBits &= ~INIT_BIT_JOIN_MATCH;
      return this;
    }

    /**
     * Builds a new {@link ImmutableAsOfJoinTable ImmutableAsOfJoinTable}.
     * @return An immutable instance of AsOfJoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAsOfJoinTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableAsOfJoinTable.validate(new ImmutableAsOfJoinTable(
          left,
          right,
          createUnmodifiableList(true, matches),
          createUnmodifiableList(true, additions),
          joinMatch));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
      if ((initBits & INIT_BIT_JOIN_MATCH) != 0) attributes.add("joinMatch");
      return "Cannot build AsOfJoinTable, some of required attributes are not set " + attributes;
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
