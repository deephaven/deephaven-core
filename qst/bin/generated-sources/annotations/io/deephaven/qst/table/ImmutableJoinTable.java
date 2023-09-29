package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJoinTable.builder()}.
 */
@Generated(from = "JoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableJoinTable extends JoinTable {
  private transient final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> matches;
  private final List<JoinAddition> additions;
  private final Integer reserveBits;

  private ImmutableJoinTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> matches,
      List<JoinAddition> additions,
      Integer reserveBits) {
    this.left = left;
    this.right = right;
    this.matches = matches;
    this.additions = additions;
    this.reserveBits = reserveBits;
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
   * @return The value of the {@code reserveBits} attribute
   */
  @Override
  public OptionalInt reserveBits() {
    return reserveBits != null
        ? OptionalInt.of(reserveBits)
        : OptionalInt.empty();
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableJoinTable(newValue, this.right, this.matches, this.additions, this.reserveBits));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JoinTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJoinTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableJoinTable(this.left, newValue, this.matches, this.additions, this.reserveBits));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JoinTable#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableJoinTable(this.left, this.right, newValue, this.additions, this.reserveBits));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JoinTable#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableJoinTable(this.left, this.right, newValue, this.additions, this.reserveBits));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JoinTable#additions() additions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withAdditions(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableJoinTable(this.left, this.right, this.matches, newValue, this.reserveBits));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link JoinTable#additions() additions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of additions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withAdditions(Iterable<? extends JoinAddition> elements) {
    if (this.additions == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableJoinTable(this.left, this.right, this.matches, newValue, this.reserveBits));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JoinTable#reserveBits() reserveBits} attribute.
   * @param value The value for reserveBits
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withReserveBits(int value) {
    Integer newValue = value;
    if (Objects.equals(this.reserveBits, newValue)) return this;
    return validate(new ImmutableJoinTable(this.left, this.right, this.matches, this.additions, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JoinTable#reserveBits() reserveBits} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for reserveBits
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJoinTable withReserveBits(OptionalInt optional) {
    Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.reserveBits, value)) return this;
    return validate(new ImmutableJoinTable(this.left, this.right, this.matches, this.additions, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJoinTable
        && equalTo(0, (ImmutableJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableJoinTable another) {
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && matches.equals(another.matches)
        && additions.equals(another.additions)
        && Objects.equals(reserveBits, another.reserveBits);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code additions}, {@code reserveBits}.
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
    h += (h << 5) + Objects.hashCode(reserveBits);
    return h;
  }

  private static ImmutableJoinTable validate(ImmutableJoinTable instance) {
    instance.checkAdditions();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link JoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JoinTable instance
   */
  public static ImmutableJoinTable copyOf(JoinTable instance) {
    if (instance instanceof ImmutableJoinTable) {
      return (ImmutableJoinTable) instance;
    }
    return ImmutableJoinTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJoinTable ImmutableJoinTable}.
   * <pre>
   * ImmutableJoinTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link JoinTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link JoinTable#right() right}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link JoinTable#matches() matches} elements
   *    .addAdditions|addAllAdditions(io.deephaven.api.JoinAddition) // {@link JoinTable#additions() additions} elements
   *    .reserveBits(int) // optional {@link JoinTable#reserveBits() reserveBits}
   *    .build();
   * </pre>
   * @return A new ImmutableJoinTable builder
   */
  public static ImmutableJoinTable.Builder builder() {
    return new ImmutableJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJoinTable ImmutableJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JoinTable", generator = "Immutables")
  public static final class Builder implements JoinTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private long initBits = 0x3L;

    private TableSpec left;
    private TableSpec right;
    private List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private List<JoinAddition> additions = new ArrayList<JoinAddition>();
    private Integer reserveBits;

    private Builder() {
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.JoinTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(JoinTable instance) {
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
      if (object instanceof JoinTable) {
        JoinTable instance = (JoinTable) object;
        if ((bits & 0x4L) == 0) {
          right(instance.right());
          bits |= 0x4L;
        }
        if ((bits & 0x1L) == 0) {
          addAllAdditions(instance.additions());
          bits |= 0x1L;
        }
        OptionalInt reserveBitsOptional = instance.reserveBits();
        if (reserveBitsOptional.isPresent()) {
          reserveBits(reserveBitsOptional);
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
     * Initializes the value for the {@link JoinTable#left() left} attribute.
     * @param left The value for left 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder left(TableSpec left) {
      this.left = Objects.requireNonNull(left, "left");
      initBits &= ~INIT_BIT_LEFT;
      return this;
    }

    /**
     * Initializes the value for the {@link JoinTable#right() right} attribute.
     * @param right The value for right 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder right(TableSpec right) {
      this.right = Objects.requireNonNull(right, "right");
      initBits &= ~INIT_BIT_RIGHT;
      return this;
    }

    /**
     * Adds one element to {@link JoinTable#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link JoinTable#matches() matches} list.
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
     * Sets or replaces all elements for {@link JoinTable#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder matches(Iterable<? extends JoinMatch> elements) {
      this.matches.clear();
      return addAllMatches(elements);
    }

    /**
     * Adds elements to {@link JoinTable#matches() matches} list.
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
     * Adds one element to {@link JoinTable#additions() additions} list.
     * @param element A additions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAdditions(JoinAddition element) {
      this.additions.add(Objects.requireNonNull(element, "additions element"));
      return this;
    }

    /**
     * Adds elements to {@link JoinTable#additions() additions} list.
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
     * Sets or replaces all elements for {@link JoinTable#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder additions(Iterable<? extends JoinAddition> elements) {
      this.additions.clear();
      return addAllAdditions(elements);
    }

    /**
     * Adds elements to {@link JoinTable#additions() additions} list.
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
     * Initializes the optional value {@link JoinTable#reserveBits() reserveBits} to reserveBits.
     * @param reserveBits The value for reserveBits
     * @return {@code this} builder for chained invocation
     */
    public final Builder reserveBits(int reserveBits) {
      this.reserveBits = reserveBits;
      return this;
    }

    /**
     * Initializes the optional value {@link JoinTable#reserveBits() reserveBits} to reserveBits.
     * @param reserveBits The value for reserveBits
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder reserveBits(OptionalInt reserveBits) {
      this.reserveBits = reserveBits.isPresent() ? reserveBits.getAsInt() : null;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJoinTable ImmutableJoinTable}.
     * @return An immutable instance of JoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJoinTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableJoinTable.validate(new ImmutableJoinTable(
          left,
          right,
          createUnmodifiableList(true, matches),
          createUnmodifiableList(true, additions),
          reserveBits));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LEFT) != 0) attributes.add("left");
      if ((initBits & INIT_BIT_RIGHT) != 0) attributes.add("right");
      return "Cannot build JoinTable, some of required attributes are not set " + attributes;
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
