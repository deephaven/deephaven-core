package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.ReverseAsOfJoinRule;
import java.io.ObjectStreamException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ReverseAsOfJoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableReverseAsOfJoinTable.builder()}.
 */
@Generated(from = "ReverseAsOfJoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableReverseAsOfJoinTable extends ReverseAsOfJoinTable {
  private final int depth;
  private final TableSpec left;
  private final TableSpec right;
  private final List<JoinMatch> matches;
  private final List<JoinAddition> additions;
  private final ReverseAsOfJoinRule rule;
  private final int hashCode;

  private ImmutableReverseAsOfJoinTable(ImmutableReverseAsOfJoinTable.Builder builder) {
    this.left = builder.left;
    this.right = builder.right;
    this.matches = createUnmodifiableList(true, builder.matches);
    this.additions = createUnmodifiableList(true, builder.additions);
    if (builder.ruleIsSet()) {
      initShim.rule(builder.rule);
    }
    this.depth = initShim.depth();
    this.rule = initShim.rule();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private ImmutableReverseAsOfJoinTable(
      TableSpec left,
      TableSpec right,
      List<JoinMatch> matches,
      List<JoinAddition> additions,
      ReverseAsOfJoinRule rule) {
    this.left = left;
    this.right = right;
    this.matches = matches;
    this.additions = additions;
    initShim.rule(rule);
    this.depth = initShim.depth();
    this.rule = initShim.rule();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ReverseAsOfJoinTable", generator = "Immutables")
  private final class InitShim {
    private byte depthBuildStage = STAGE_UNINITIALIZED;
    private int depth;

    int depth() {
      if (depthBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (depthBuildStage == STAGE_UNINITIALIZED) {
        depthBuildStage = STAGE_INITIALIZING;
        this.depth = ImmutableReverseAsOfJoinTable.super.depth();
        depthBuildStage = STAGE_INITIALIZED;
      }
      return this.depth;
    }

    private byte ruleBuildStage = STAGE_UNINITIALIZED;
    private ReverseAsOfJoinRule rule;

    ReverseAsOfJoinRule rule() {
      if (ruleBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (ruleBuildStage == STAGE_UNINITIALIZED) {
        ruleBuildStage = STAGE_INITIALIZING;
        this.rule = Objects.requireNonNull(ImmutableReverseAsOfJoinTable.super.rule(), "rule");
        ruleBuildStage = STAGE_INITIALIZED;
      }
      return this.rule;
    }

    void rule(ReverseAsOfJoinRule rule) {
      this.rule = rule;
      ruleBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (depthBuildStage == STAGE_INITIALIZING) attributes.add("depth");
      if (ruleBuildStage == STAGE_INITIALIZING) attributes.add("rule");
      return "Cannot build ReverseAsOfJoinTable, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.depth()
        : this.depth;
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
   * @return The value of the {@code rule} attribute
   */
  @Override
  public ReverseAsOfJoinRule rule() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.rule()
        : this.rule;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReverseAsOfJoinTable#left() left} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for left
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withLeft(TableSpec value) {
    if (this.left == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "left");
    return validate(new ImmutableReverseAsOfJoinTable(newValue, this.right, this.matches, this.additions, this.rule));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReverseAsOfJoinTable#right() right} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for right
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withRight(TableSpec value) {
    if (this.right == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "right");
    return validate(new ImmutableReverseAsOfJoinTable(this.left, newValue, this.matches, this.additions, this.rule));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ReverseAsOfJoinTable#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, newValue, this.additions, this.rule));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ReverseAsOfJoinTable#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, newValue, this.additions, this.rule));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ReverseAsOfJoinTable#additions() additions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withAdditions(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, this.matches, newValue, this.rule));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ReverseAsOfJoinTable#additions() additions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of additions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withAdditions(Iterable<? extends JoinAddition> elements) {
    if (this.additions == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, this.matches, newValue, this.rule));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReverseAsOfJoinTable#rule() rule} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rule
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReverseAsOfJoinTable withRule(ReverseAsOfJoinRule value) {
    ReverseAsOfJoinRule newValue = Objects.requireNonNull(value, "rule");
    if (this.rule == newValue) return this;
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, this.matches, this.additions, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableReverseAsOfJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableReverseAsOfJoinTable
        && equalTo(0, (ImmutableReverseAsOfJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableReverseAsOfJoinTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && left.equals(another.left)
        && right.equals(another.right)
        && matches.equals(another.matches)
        && additions.equals(another.additions)
        && rule.equals(another.rule);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code left}, {@code right}, {@code matches}, {@code additions}, {@code rule}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + left.hashCode();
    h += (h << 5) + right.hashCode();
    h += (h << 5) + matches.hashCode();
    h += (h << 5) + additions.hashCode();
    h += (h << 5) + rule.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableReverseAsOfJoinTable, WeakReference<ImmutableReverseAsOfJoinTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableReverseAsOfJoinTable validate(ImmutableReverseAsOfJoinTable instance) {
    instance.checkAdditions();
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableReverseAsOfJoinTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableReverseAsOfJoinTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link ReverseAsOfJoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ReverseAsOfJoinTable instance
   */
  public static ImmutableReverseAsOfJoinTable copyOf(ReverseAsOfJoinTable instance) {
    if (instance instanceof ImmutableReverseAsOfJoinTable) {
      return (ImmutableReverseAsOfJoinTable) instance;
    }
    return ImmutableReverseAsOfJoinTable.builder()
        .left(instance.left())
        .right(instance.right())
        .addAllMatches(instance.matches())
        .addAllAdditions(instance.additions())
        .rule(instance.rule())
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(new ImmutableReverseAsOfJoinTable(this.left, this.right, this.matches, this.additions, this.rule));
  }

  /**
   * Creates a builder for {@link ImmutableReverseAsOfJoinTable ImmutableReverseAsOfJoinTable}.
   * <pre>
   * ImmutableReverseAsOfJoinTable.builder()
   *    .left(io.deephaven.qst.table.TableSpec) // required {@link ReverseAsOfJoinTable#left() left}
   *    .right(io.deephaven.qst.table.TableSpec) // required {@link ReverseAsOfJoinTable#right() right}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link ReverseAsOfJoinTable#matches() matches} elements
   *    .addAdditions|addAllAdditions(io.deephaven.api.JoinAddition) // {@link ReverseAsOfJoinTable#additions() additions} elements
   *    .rule(io.deephaven.api.ReverseAsOfJoinRule) // optional {@link ReverseAsOfJoinTable#rule() rule}
   *    .build();
   * </pre>
   * @return A new ImmutableReverseAsOfJoinTable builder
   */
  public static ImmutableReverseAsOfJoinTable.Builder builder() {
    return new ImmutableReverseAsOfJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableReverseAsOfJoinTable ImmutableReverseAsOfJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ReverseAsOfJoinTable", generator = "Immutables")
  public static final class Builder implements ReverseAsOfJoinTable.Builder {
    private static final long INIT_BIT_LEFT = 0x1L;
    private static final long INIT_BIT_RIGHT = 0x2L;
    private static final long OPT_BIT_RULE = 0x1L;
    private long initBits = 0x3L;
    private long optBits;

    private TableSpec left;
    private TableSpec right;
    private final List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private final List<JoinAddition> additions = new ArrayList<JoinAddition>();
    private ReverseAsOfJoinRule rule;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link ReverseAsOfJoinTable#left() left} attribute.
     * @param left The value for left 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder left(TableSpec left) {
      checkNotIsSet(leftIsSet(), "left");
      this.left = Objects.requireNonNull(left, "left");
      initBits &= ~INIT_BIT_LEFT;
      return this;
    }

    /**
     * Initializes the value for the {@link ReverseAsOfJoinTable#right() right} attribute.
     * @param right The value for right 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder right(TableSpec right) {
      checkNotIsSet(rightIsSet(), "right");
      this.right = Objects.requireNonNull(right, "right");
      initBits &= ~INIT_BIT_RIGHT;
      return this;
    }

    /**
     * Adds one element to {@link ReverseAsOfJoinTable#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link ReverseAsOfJoinTable#matches() matches} list.
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
     * Adds elements to {@link ReverseAsOfJoinTable#matches() matches} list.
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
     * Adds one element to {@link ReverseAsOfJoinTable#additions() additions} list.
     * @param element A additions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAdditions(JoinAddition element) {
      this.additions.add(Objects.requireNonNull(element, "additions element"));
      return this;
    }

    /**
     * Adds elements to {@link ReverseAsOfJoinTable#additions() additions} list.
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
     * Adds elements to {@link ReverseAsOfJoinTable#additions() additions} list.
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
     * Initializes the value for the {@link ReverseAsOfJoinTable#rule() rule} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ReverseAsOfJoinTable#rule() rule}.</em>
     * @param rule The value for rule 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder rule(ReverseAsOfJoinRule rule) {
      checkNotIsSet(ruleIsSet(), "rule");
      this.rule = Objects.requireNonNull(rule, "rule");
      optBits |= OPT_BIT_RULE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableReverseAsOfJoinTable ImmutableReverseAsOfJoinTable}.
     * @return An immutable instance of ReverseAsOfJoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableReverseAsOfJoinTable build() {
      checkRequiredAttributes();
      return ImmutableReverseAsOfJoinTable.validate(new ImmutableReverseAsOfJoinTable(this));
    }

    private boolean ruleIsSet() {
      return (optBits & OPT_BIT_RULE) != 0;
    }

    private boolean leftIsSet() {
      return (initBits & INIT_BIT_LEFT) == 0;
    }

    private boolean rightIsSet() {
      return (initBits & INIT_BIT_RIGHT) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of ReverseAsOfJoinTable is strict, attribute is already set: ".concat(name));
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
      return "Cannot build ReverseAsOfJoinTable, some of required attributes are not set " + attributes;
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
