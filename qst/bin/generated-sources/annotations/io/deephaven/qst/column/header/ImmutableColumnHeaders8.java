package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders8}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders8.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders8.of()}.
 */
@Generated(from = "ColumnHeaders8", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8>
    extends ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> {
  private final ColumnHeader<T8> header8;
  private final ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others;

  private ImmutableColumnHeaders8(
      ColumnHeader<T8> header8,
      ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others) {
    this.header8 = Objects.requireNonNull(header8, "header8");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders8(
      ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> original,
      ColumnHeader<T8> header8,
      ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others) {
    this.header8 = header8;
    this.others = others;
  }

  /**
   * @return The value of the {@code header8} attribute
   */
  @Override
  public ColumnHeader<T8> header8() {
    return header8;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders8#header8() header8} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header8
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> withHeader8(ColumnHeader<T8> value) {
    if (this.header8 == value) return this;
    ColumnHeader<T8> newValue = Objects.requireNonNull(value, "header8");
    return new ImmutableColumnHeaders8<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders8#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> withOthers(ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> value) {
    if (this.others == value) return this;
    ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders8<>(this, this.header8, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders8} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders8<?, ?, ?, ?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders8<?, ?, ?, ?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders8<?, ?, ?, ?, ?, ?, ?, ?> another) {
    return header8.equals(another.header8)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header8}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header8.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders8} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders8{"
        + "header8=" + header8
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders8} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
 * @param <T5> generic parameter T5
 * @param <T6> generic parameter T6
 * @param <T7> generic parameter T7
 * @param <T8> generic parameter T8
   * @param header8 The value for the {@code header8} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders8 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8> ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> of(ColumnHeader<T8> header8, ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others) {
    return new ImmutableColumnHeaders8<>(header8, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders8} value.
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
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders8 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8> ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> copyOf(ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> instance) {
    if (instance instanceof ImmutableColumnHeaders8<?, ?, ?, ?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8>) instance;
    }
    return ImmutableColumnHeaders8.<T1, T2, T3, T4, T5, T6, T7, T8>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders8 ImmutableColumnHeaders8}.
   * <pre>
   * ImmutableColumnHeaders8.&amp;lt;T1, T2, T3, T4, T5, T6, T7, T8&amp;gt;builder()
   *    .header8(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T8&amp;gt;) // required {@link ColumnHeaders8#header8() header8}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders7&amp;lt;T1, T2, T3, T4, T5, T6, T7&amp;gt;) // required {@link ColumnHeaders8#others() others}
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
   * @return A new ImmutableColumnHeaders8 builder
   */
  public static <T1, T2, T3, T4, T5, T6, T7, T8> ImmutableColumnHeaders8.Builder<T1, T2, T3, T4, T5, T6, T7, T8> builder() {
    return new ImmutableColumnHeaders8.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders8 ImmutableColumnHeaders8}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders8", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5, T6, T7, T8> {
    private static final long INIT_BIT_HEADER8 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T8> header8;
    private ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders8} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8> from(ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> instance) {
      Objects.requireNonNull(instance, "instance");
      header8(instance.header8());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders8#header8() header8} attribute.
     * @param header8 The value for header8 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8> header8(ColumnHeader<T8> header8) {
      this.header8 = Objects.requireNonNull(header8, "header8");
      initBits &= ~INIT_BIT_HEADER8;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders8#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7, T8> others(ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders8 ImmutableColumnHeaders8}.
     * @return An immutable instance of ColumnHeaders8
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders8<>(null, header8, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER8) != 0) attributes.add("header8");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders8, some of required attributes are not set " + attributes;
    }
  }
}
