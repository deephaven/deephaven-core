package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeadersN}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeadersN.builder()}.
 */
@Generated(from = "ColumnHeadersN", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9>
    extends ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
  private final List<ColumnHeader<?>> headers;
  private final ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others;

  private ImmutableColumnHeadersN(
      List<ColumnHeader<?>> headers,
      ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others) {
    this.headers = headers;
    this.others = others;
  }

  /**
   * @return The value of the {@code headers} attribute
   */
  @Override
  public List<ColumnHeader<?>> headers() {
    return headers;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others() {
    return others;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnHeadersN#headers() headers}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  @SafeVarargs @SuppressWarnings("varargs")
  public final ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> withHeaders(ColumnHeader<?>... elements) {
    List<ColumnHeader<?>> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableColumnHeadersN<>(newValue, this.others));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ColumnHeadersN#headers() headers}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of headers elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> withHeaders(Iterable<? extends ColumnHeader<?>> elements) {
    if (this.headers == elements) return this;
    List<ColumnHeader<?>> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableColumnHeadersN<>(newValue, this.others));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeadersN#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> withOthers(ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> value) {
    if (this.others == value) return this;
    ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> newValue = Objects.requireNonNull(value, "others");
    return validate(new ImmutableColumnHeadersN<>(this.headers, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeadersN} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeadersN<?, ?, ?, ?, ?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeadersN<?, ?, ?, ?, ?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeadersN<?, ?, ?, ?, ?, ?, ?, ?, ?> another) {
    return headers.equals(another.headers)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code headers}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + headers.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeadersN} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeadersN{"
        + "headers=" + headers
        + ", others=" + others
        + "}";
  }

  private static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> validate(ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> instance) {
    instance.checkSize();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeadersN} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @param <T7> generic parameter T7
   * @param <T8> generic parameter T8
   * @param <T9> generic parameter T9
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeadersN instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> copyOf(ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> instance) {
    if (instance instanceof ImmutableColumnHeadersN<?, ?, ?, ?, ?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9>) instance;
    }
    return ImmutableColumnHeadersN.<T1, T2, T3, T4, T5, T6, T7, T8, T9>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeadersN ImmutableColumnHeadersN}.
   * <pre>
   * ImmutableColumnHeadersN.&amp;lt;T1, T2, T3, T4, T5, T6, T7, T8, T9&amp;gt;builder()
   *    .addHeaders|addAllHeaders(io.deephaven.qst.column.header.ColumnHeader&amp;lt;?&amp;gt;) // {@link ColumnHeadersN#headers() headers} elements
   *    .others(io.deephaven.qst.column.header.ColumnHeaders9&amp;lt;T1, T2, T3, T4, T5, T6, T7, T8, T9&amp;gt;) // required {@link ColumnHeadersN#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @param <T7> generic parameter T7
   * @param <T8> generic parameter T8
   * @param <T9> generic parameter T9
   * @return A new ImmutableColumnHeadersN builder
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeadersN.Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> builder() {
    return new ImmutableColumnHeadersN.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeadersN ImmutableColumnHeadersN}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeadersN", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
    private static final long INIT_BIT_OTHERS = 0x1L;
    private long initBits = 0x1L;

    private List<ColumnHeader<?>> headers = new ArrayList<ColumnHeader<?>>();
    private ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeadersN} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> from(ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> instance) {
      Objects.requireNonNull(instance, "instance");
      addAllHeaders(instance.headers());
      others(instance.others());
      return this;
    }

    /**
     * Adds one element to {@link ColumnHeadersN#headers() headers} list.
     * @param element A headers element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> addHeaders(ColumnHeader<?> element) {
      this.headers.add(Objects.requireNonNull(element, "headers element"));
      return this;
    }

    /**
     * Adds elements to {@link ColumnHeadersN#headers() headers} list.
     * @param elements An array of headers elements
     * @return {@code this} builder for use in a chained invocation
     */
    @SafeVarargs @SuppressWarnings("varargs")
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> addHeaders(ColumnHeader<?>... elements) {
      for (ColumnHeader<?> element : elements) {
        this.headers.add(Objects.requireNonNull(element, "headers element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ColumnHeadersN#headers() headers} list.
     * @param elements An iterable of headers elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> headers(Iterable<? extends ColumnHeader<?>> elements) {
      this.headers.clear();
      return addAllHeaders(elements);
    }

    /**
     * Adds elements to {@link ColumnHeadersN#headers() headers} list.
     * @param elements An iterable of headers elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> addAllHeaders(Iterable<? extends ColumnHeader<?>> elements) {
      for (ColumnHeader<?> element : elements) {
        this.headers.add(Objects.requireNonNull(element, "headers element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeadersN#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> others(ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeadersN ImmutableColumnHeadersN}.
     * @return An immutable instance of ColumnHeadersN
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumnHeadersN.validate(new ImmutableColumnHeadersN<>(createUnmodifiableList(true, headers), others));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeadersN, some of required attributes are not set " + attributes;
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
