package io.deephaven.api.updateby;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.spec.UpdateBySpec;
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
 * Immutable implementation of {@link ColumnUpdateOperation}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnUpdateOperation.builder()}.
 */
@Generated(from = "ColumnUpdateOperation", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableColumnUpdateOperation extends ColumnUpdateOperation {
  private final UpdateBySpec spec;
  private final List<Pair> columns;

  private ImmutableColumnUpdateOperation(UpdateBySpec spec, List<Pair> columns) {
    this.spec = spec;
    this.columns = columns;
  }

  /**
   * Provide the specification for an updateBy operation.
   */
  @Override
  public UpdateBySpec spec() {
    return spec;
  }

  /**
   * Provide the list of {@link Pair}s for the result columns. If `columns()` is not provided, internally will create
   * a new list mapping each source column 1:1 to output columns (where applicable)
   */
  @Override
  public List<Pair> columns() {
    return columns;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnUpdateOperation#spec() spec} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for spec
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnUpdateOperation withSpec(UpdateBySpec value) {
    if (this.spec == value) return this;
    UpdateBySpec newValue = Objects.requireNonNull(value, "spec");
    return new ImmutableColumnUpdateOperation(newValue, this.columns);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnUpdateOperation#columns() columns}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnUpdateOperation withColumns(Pair... elements) {
    List<Pair> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableColumnUpdateOperation(this.spec, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnUpdateOperation#columns() columns}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of columns elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnUpdateOperation withColumns(Iterable<? extends Pair> elements) {
    if (this.columns == elements) return this;
    List<Pair> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableColumnUpdateOperation(this.spec, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnUpdateOperation} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnUpdateOperation
        && equalTo(0, (ImmutableColumnUpdateOperation) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnUpdateOperation another) {
    return spec.equals(another.spec)
        && columns.equals(another.columns);
  }

  /**
   * Computes a hash code from attributes: {@code spec}, {@code columns}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + spec.hashCode();
    h += (h << 5) + columns.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnUpdateOperation} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnUpdateOperation{"
        + "spec=" + spec
        + ", columns=" + columns
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ColumnUpdateOperation} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ColumnUpdateOperation instance
   */
  public static ImmutableColumnUpdateOperation copyOf(ColumnUpdateOperation instance) {
    if (instance instanceof ImmutableColumnUpdateOperation) {
      return (ImmutableColumnUpdateOperation) instance;
    }
    return ImmutableColumnUpdateOperation.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnUpdateOperation ImmutableColumnUpdateOperation}.
   * <pre>
   * ImmutableColumnUpdateOperation.builder()
   *    .spec(io.deephaven.api.updateby.spec.UpdateBySpec) // required {@link ColumnUpdateOperation#spec() spec}
   *    .addColumns|addAllColumns(io.deephaven.api.agg.Pair) // {@link ColumnUpdateOperation#columns() columns} elements
   *    .build();
   * </pre>
   * @return A new ImmutableColumnUpdateOperation builder
   */
  public static ImmutableColumnUpdateOperation.Builder builder() {
    return new ImmutableColumnUpdateOperation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableColumnUpdateOperation ImmutableColumnUpdateOperation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnUpdateOperation", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ColumnUpdateOperation.Builder {
    private static final long INIT_BIT_SPEC = 0x1L;
    private long initBits = 0x1L;

    private @Nullable UpdateBySpec spec;
    private List<Pair> columns = new ArrayList<Pair>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnUpdateOperation} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ColumnUpdateOperation instance) {
      Objects.requireNonNull(instance, "instance");
      spec(instance.spec());
      addAllColumns(instance.columns());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnUpdateOperation#spec() spec} attribute.
     * @param spec The value for spec 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder spec(UpdateBySpec spec) {
      this.spec = Objects.requireNonNull(spec, "spec");
      initBits &= ~INIT_BIT_SPEC;
      return this;
    }

    /**
     * Adds one element to {@link ColumnUpdateOperation#columns() columns} list.
     * @param element A columns element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(Pair element) {
      this.columns.add(Objects.requireNonNull(element, "columns element"));
      return this;
    }

    /**
     * Adds elements to {@link ColumnUpdateOperation#columns() columns} list.
     * @param elements An array of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addColumns(Pair... elements) {
      for (Pair element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ColumnUpdateOperation#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder columns(Iterable<? extends Pair> elements) {
      this.columns.clear();
      return addAllColumns(elements);
    }

    /**
     * Adds elements to {@link ColumnUpdateOperation#columns() columns} list.
     * @param elements An iterable of columns elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllColumns(Iterable<? extends Pair> elements) {
      for (Pair element : elements) {
        this.columns.add(Objects.requireNonNull(element, "columns element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnUpdateOperation ImmutableColumnUpdateOperation}.
     * @return An immutable instance of ColumnUpdateOperation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnUpdateOperation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnUpdateOperation(spec, createUnmodifiableList(true, columns));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SPEC) != 0) attributes.add("spec");
      return "Cannot build ColumnUpdateOperation, some of required attributes are not set " + attributes;
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
