package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MultiJoinTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMultiJoinTable.builder()}.
 */
@Generated(from = "MultiJoinTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableMultiJoinTable extends MultiJoinTable {
  private transient final int depth;
  private final List<MultiJoinInput<TableSpec>> inputs;

  private ImmutableMultiJoinTable(List<MultiJoinInput<TableSpec>> inputs) {
    this.inputs = inputs;
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
   * @return The value of the {@code inputs} attribute
   */
  @Override
  public List<MultiJoinInput<TableSpec>> inputs() {
    return inputs;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinTable#inputs() inputs}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  @SafeVarargs @SuppressWarnings("varargs")
  public final ImmutableMultiJoinTable withInputs(MultiJoinInput<TableSpec>... elements) {
    List<MultiJoinInput<TableSpec>> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableMultiJoinTable(newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link MultiJoinTable#inputs() inputs}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of inputs elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableMultiJoinTable withInputs(Iterable<? extends MultiJoinInput<TableSpec>> elements) {
    if (this.inputs == elements) return this;
    List<MultiJoinInput<TableSpec>> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableMultiJoinTable(newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMultiJoinTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMultiJoinTable
        && equalTo(0, (ImmutableMultiJoinTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableMultiJoinTable another) {
    return depth == another.depth
        && inputs.equals(another.inputs);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code inputs}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + inputs.hashCode();
    return h;
  }

  private static ImmutableMultiJoinTable validate(ImmutableMultiJoinTable instance) {
    instance.checkAdditions();
    instance.checkInputs();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link MultiJoinTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable MultiJoinTable instance
   */
  public static ImmutableMultiJoinTable copyOf(MultiJoinTable instance) {
    if (instance instanceof ImmutableMultiJoinTable) {
      return (ImmutableMultiJoinTable) instance;
    }
    return ImmutableMultiJoinTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableMultiJoinTable ImmutableMultiJoinTable}.
   * <pre>
   * ImmutableMultiJoinTable.builder()
   *    .addInputs|addAllInputs(io.deephaven.qst.table.MultiJoinInput&amp;lt;io.deephaven.qst.table.TableSpec&amp;gt;) // {@link MultiJoinTable#inputs() inputs} elements
   *    .build();
   * </pre>
   * @return A new ImmutableMultiJoinTable builder
   */
  public static ImmutableMultiJoinTable.Builder builder() {
    return new ImmutableMultiJoinTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMultiJoinTable ImmutableMultiJoinTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MultiJoinTable", generator = "Immutables")
  public static final class Builder implements MultiJoinTable.Builder {
    private List<MultiJoinInput<TableSpec>> inputs = new ArrayList<MultiJoinInput<TableSpec>>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code MultiJoinTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(MultiJoinTable instance) {
      Objects.requireNonNull(instance, "instance");
      addAllInputs(instance.inputs());
      return this;
    }

    /**
     * Adds one element to {@link MultiJoinTable#inputs() inputs} list.
     * @param element A inputs element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addInputs(MultiJoinInput<TableSpec> element) {
      this.inputs.add(Objects.requireNonNull(element, "inputs element"));
      return this;
    }

    /**
     * Adds elements to {@link MultiJoinTable#inputs() inputs} list.
     * @param elements An array of inputs elements
     * @return {@code this} builder for use in a chained invocation
     */
    @SafeVarargs @SuppressWarnings("varargs")
    public final Builder addInputs(MultiJoinInput<TableSpec>... elements) {
      for (MultiJoinInput<TableSpec> element : elements) {
        this.inputs.add(Objects.requireNonNull(element, "inputs element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link MultiJoinTable#inputs() inputs} list.
     * @param elements An iterable of inputs elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder inputs(Iterable<? extends MultiJoinInput<TableSpec>> elements) {
      this.inputs.clear();
      return addAllInputs(elements);
    }

    /**
     * Adds elements to {@link MultiJoinTable#inputs() inputs} list.
     * @param elements An iterable of inputs elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllInputs(Iterable<? extends MultiJoinInput<TableSpec>> elements) {
      for (MultiJoinInput<TableSpec> element : elements) {
        this.inputs.add(Objects.requireNonNull(element, "inputs element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableMultiJoinTable ImmutableMultiJoinTable}.
     * @return An immutable instance of MultiJoinTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMultiJoinTable build() {
      return ImmutableMultiJoinTable.validate(new ImmutableMultiJoinTable(createUnmodifiableList(true, inputs)));
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
