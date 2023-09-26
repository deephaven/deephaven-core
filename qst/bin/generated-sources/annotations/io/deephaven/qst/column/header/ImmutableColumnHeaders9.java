package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders9}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders9.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders9.of()}.
 */
@Generated(from = "ColumnHeaders9", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9>
    extends ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
  private final ColumnHeader<T9> header9;
  private final ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others;

  private ImmutableColumnHeaders9(
      ColumnHeader<T9> header9,
      ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others) {
    this.header9 = Objects.requireNonNull(header9, "header9");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders9(
      ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> original,
      ColumnHeader<T9> header9,
      ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others) {
    this.header9 = header9;
    this.others = others;
  }

  /**
   * @return The value of the {@code header9} attribute
   */
  @Override
  public ColumnHeader<T9> header9() {
    return header9;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders9#header9() header9} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header9
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> withHeader9(ColumnHeader<T9> value) {
    if (this.header9 == value) return this;
    ColumnHeader<T9> newValue = Objects.requireNonNull(value, "header9");
    return new ImmutableColumnHeaders9<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders9#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> withOthers(ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> value) {
    if (this.others == value) return this;
    ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders9<>(this, this.header9, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders9} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders9<?, ?, ?, ?, ?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders9<?, ?, ?, ?, ?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders9<?, ?, ?, ?, ?, ?, ?, ?, ?> another) {
    return header9.equals(another.header9)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header9}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header9.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders9} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders9{"
        + "header9=" + header9
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders9} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
 * @param <T5> generic parameter T5
 * @param <T6> generic parameter T6
 * @param <T7> generic parameter T7
 * @param <T8> generic parameter T8
 * @param <T9> generic parameter T9
   * @param header9 The value for the {@code header9} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders9 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> of(ColumnHeader<T9> header9, ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others) {
    return new ImmutableColumnHeaders9<>(header9, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders9} value.
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
   * @return A copied immutable ColumnHeaders9 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> copyOf(ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> instance) {
    if (instance instanceof ImmutableColumnHeaders9<?, ?, ?, ?, ?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9>) instance;
    }
    return ImmutableColumnHeaders9.<T1, T2, T3, T4, T5, T6, T7, T8, T9>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders9 ImmutableColumnHeaders9}.
   * <pre>
   * ImmutableColumnHeaders9.&amp;lt;T1, T2, T3, T4, T5, T6, T7, T8, T9&amp;gt;builder()
   *    .header9(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T9&amp;gt;) // required {@link ColumnHeaders9#header9() header9}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders8&amp;lt;T1, T2, T3, T4, T5, T6, T7, T8&amp;gt;) // required {@link ColumnHeaders9#others() others}
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
   * @return A new ImmutableColumnHeaders9 builder
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ImmutableColumnHeaders9.Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> builder() {
    return new ImmutableColumnHeaders9.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders9 ImmutableColumnHeaders9}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders9", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
    private static final long INIT_BIT_HEADER9 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T9> header9;
    private ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders9} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> from(ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> instance) {
      Objects.requireNonNull(instance, "instance");
      header9(instance.header9());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders9#header9() header9} attribute.
     * @param header9 The value for header9 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> header9(ColumnHeader<T9> header9) {
      this.header9 = Objects.requireNonNull(header9, "header9");
      initBits &= ~INIT_BIT_HEADER9;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders9#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8, T9> others(ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders9 ImmutableColumnHeaders9}.
     * @return An immutable instance of ColumnHeaders9
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders9<>(null, header9, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER9) != 0) attributes.add("header9");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders9, some of required attributes are not set " + attributes;
    }
  }
}
