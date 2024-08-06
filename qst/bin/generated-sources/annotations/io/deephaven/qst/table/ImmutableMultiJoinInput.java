package io.deephaven.qst.table;

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
 * Immutable implementation of {@link MultiJoinInput}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMultiJoinInput.builder()}.
 */
@Generated(from = "MultiJoinInput", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableMultiJoinInput<T> extends MultiJoinInput<T> {
  private final T table;
  private final List<JoinMatch> matches;
  private final List<JoinAddition> additions;

  private ImmutableMultiJoinInput(
      T table,
      List<JoinMatch> matches,
      List<JoinAddition> additions) {
    this.table = table;
    this.matches = matches;
    this.additions = additions;
  }

  /**
   * @return The value of the {@code table} attribute
   */
  @Override
  public T table() {
    return table;
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
   * Copy the current immutable object by setting a value for the {@link MultiJoinInput#table() table} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for table
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableMultiJoinInput<T> withTable(T value) {
    if (this.table == value) return this;
    T newValue = Objects.requireNonNull(value, "table");
    return validate(new ImmutableMultiJoinInput<>(newValue, this.matches, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#matches() matches}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput<T> withMatches(JoinMatch... elements) {
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableMultiJoinInput<>(this.table, newValue, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#matches() matches}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of matches elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput<T> withMatches(Iterable<? extends JoinMatch> elements) {
    if (this.matches == elements) return this;
    List<JoinMatch> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableMultiJoinInput<>(this.table, newValue, this.additions));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#additions() additions}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput<T> withAdditions(JoinAddition... elements) {
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableMultiJoinInput<>(this.table, this.matches, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinInput#additions() additions}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of additions elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinInput<T> withAdditions(Iterable<? extends JoinAddition> elements) {
    if (this.additions == elements) return this;
    List<JoinAddition> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableMultiJoinInput<>(this.table, this.matches, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMultiJoinInput} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMultiJoinInput<?>
        && equalTo(0, (ImmutableMultiJoinInput<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableMultiJoinInput<?> another) {
    return table.equals(another.table)
        && matches.equals(another.matches)
        && additions.equals(another.additions);
  }

  /**
   * Computes a hash code from attributes: {@code table}, {@code matches}, {@code additions}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + table.hashCode();
    h += (h << 5) + matches.hashCode();
    h += (h << 5) + additions.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code MultiJoinInput} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "MultiJoinInput{"
        + "table=" + table
        + ", matches=" + matches
        + ", additions=" + additions
        + "}";
  }

  private static <T> ImmutableMultiJoinInput<T> validate(ImmutableMultiJoinInput<T> instance) {
    instance.checkAdditions();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link MultiJoinInput} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable MultiJoinInput instance
   */
  public static <T> ImmutableMultiJoinInput<T> copyOf(MultiJoinInput<T> instance) {
    if (instance instanceof ImmutableMultiJoinInput<?>) {
      return (ImmutableMultiJoinInput<T>) instance;
    }
    return ImmutableMultiJoinInput.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
   * <pre>
   * ImmutableMultiJoinInput.&amp;lt;T&amp;gt;builder()
   *    .table(T) // required {@link MultiJoinInput#table() table}
   *    .addMatches|addAllMatches(io.deephaven.api.JoinMatch) // {@link MultiJoinInput#matches() matches} elements
   *    .addAdditions|addAllAdditions(io.deephaven.api.JoinAddition) // {@link MultiJoinInput#additions() additions} elements
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableMultiJoinInput builder
   */
  public static <T> ImmutableMultiJoinInput.Builder<T> builder() {
    return new ImmutableMultiJoinInput.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MultiJoinInput", generator = "Immutables")
  public static final class Builder<T> implements MultiJoinInput.Builder<T> {
    private static final long INIT_BIT_TABLE = 0x1L;
    private long initBits = 0x1L;

    private T table;
    private List<JoinMatch> matches = new ArrayList<JoinMatch>();
    private List<JoinAddition> additions = new ArrayList<JoinAddition>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code MultiJoinInput} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(MultiJoinInput<T> instance) {
      Objects.requireNonNull(instance, "instance");
      table(instance.table());
      addAllMatches(instance.matches());
      addAllAdditions(instance.additions());
      return this;
    }

    /**
     * Initializes the value for the {@link MultiJoinInput#table() table} attribute.
     * @param table The value for table 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> table(T table) {
      this.table = Objects.requireNonNull(table, "table");
      initBits &= ~INIT_BIT_TABLE;
      return this;
    }

    /**
     * Adds one element to {@link MultiJoinInput#matches() matches} list.
     * @param element A matches element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addMatches(JoinMatch element) {
      this.matches.add(Objects.requireNonNull(element, "matches element"));
      return this;
    }

    /**
     * Adds elements to {@link MultiJoinInput#matches() matches} list.
     * @param elements An array of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addMatches(JoinMatch... elements) {
      for (JoinMatch element : elements) {
        this.matches.add(Objects.requireNonNull(element, "matches element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link MultiJoinInput#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> matches(Iterable<? extends JoinMatch> elements) {
      this.matches.clear();
      return addAllMatches(elements);
    }

    /**
     * Adds elements to {@link MultiJoinInput#matches() matches} list.
     * @param elements An iterable of matches elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAllMatches(Iterable<? extends JoinMatch> elements) {
      for (JoinMatch element : elements) {
        this.matches.add(Objects.requireNonNull(element, "matches element"));
      }
      return this;
    }

    /**
     * Adds one element to {@link MultiJoinInput#additions() additions} list.
     * @param element A additions element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAdditions(JoinAddition element) {
      this.additions.add(Objects.requireNonNull(element, "additions element"));
      return this;
    }

    /**
     * Adds elements to {@link MultiJoinInput#additions() additions} list.
     * @param elements An array of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAdditions(JoinAddition... elements) {
      for (JoinAddition element : elements) {
        this.additions.add(Objects.requireNonNull(element, "additions element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link MultiJoinInput#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> additions(Iterable<? extends JoinAddition> elements) {
      this.additions.clear();
      return addAllAdditions(elements);
    }

    /**
     * Adds elements to {@link MultiJoinInput#additions() additions} list.
     * @param elements An iterable of additions elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> addAllAdditions(Iterable<? extends JoinAddition> elements) {
      for (JoinAddition element : elements) {
        this.additions.add(Objects.requireNonNull(element, "additions element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableMultiJoinInput ImmutableMultiJoinInput}.
     * @return An immutable instance of MultiJoinInput
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMultiJoinInput<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableMultiJoinInput.validate(new ImmutableMultiJoinInput<>(table, createUnmodifiableList(true, matches), createUnmodifiableList(true, additions)));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TABLE) != 0) attributes.add("table");
      return "Cannot build MultiJoinInput, some of required attributes are not set " + attributes;
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
