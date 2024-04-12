package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders7}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders7.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders7.of()}.
 */
@Generated(from = "ColumnHeaders7", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7>
    extends ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> {
  private final ColumnHeader<T7> header7;
  private final ColumnHeaders6<T1, T2, T3, T4, T5, T6> others;

  private ImmutableColumnHeaders7(
      ColumnHeader<T7> header7,
      ColumnHeaders6<T1, T2, T3, T4, T5, T6> others) {
    this.header7 = Objects.requireNonNull(header7, "header7");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders7(
      ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> original,
      ColumnHeader<T7> header7,
      ColumnHeaders6<T1, T2, T3, T4, T5, T6> others) {
    this.header7 = header7;
    this.others = others;
  }

  /**
   * @return The value of the {@code header7} attribute
   */
  @Override
  public ColumnHeader<T7> header7() {
    return header7;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders6<T1, T2, T3, T4, T5, T6> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders7#header7() header7} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header7
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> withHeader7(ColumnHeader<T7> value) {
    if (this.header7 == value) return this;
    ColumnHeader<T7> newValue = Objects.requireNonNull(value, "header7");
    return new ImmutableColumnHeaders7<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders7#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> withOthers(ColumnHeaders6<T1, T2, T3, T4, T5, T6> value) {
    if (this.others == value) return this;
    ColumnHeaders6<T1, T2, T3, T4, T5, T6> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders7<>(this, this.header7, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders7} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders7<?, ?, ?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders7<?, ?, ?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders7<?, ?, ?, ?, ?, ?, ?> another) {
    return header7.equals(another.header7)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header7}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header7.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders7} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders7{"
        + "header7=" + header7
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders7} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
 * @param <T5> generic parameter T5
 * @param <T6> generic parameter T6
 * @param <T7> generic parameter T7
   * @param header7 The value for the {@code header7} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders7 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7> ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> of(ColumnHeader<T7> header7, ColumnHeaders6<T1, T2, T3, T4, T5, T6> others) {
    return new ImmutableColumnHeaders7<>(header7, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders7} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @param <T7> generic parameter T7
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders7 instance
   */
  public static <T1, T2, T3, T4, T5, T6, T7> ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> copyOf(ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> instance) {
    if (instance instanceof ImmutableColumnHeaders7<?, ?, ?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7>) instance;
    }
    return ImmutableColumnHeaders7.<T1, T2, T3, T4, T5, T6, T7>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders7 ImmutableColumnHeaders7}.
   * <pre>
   * ImmutableColumnHeaders7.&amp;lt;T1, T2, T3, T4, T5, T6, T7&amp;gt;builder()
   *    .header7(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T7&amp;gt;) // required {@link ColumnHeaders7#header7() header7}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders6&amp;lt;T1, T2, T3, T4, T5, T6&amp;gt;) // required {@link ColumnHeaders7#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @param <T7> generic parameter T7
   * @return A new ImmutableColumnHeaders7 builder
   */
  public static <T1, T2, T3, T4, T5, T6, T7> ImmutableColumnHeaders7.Builder<T1, T2, T3, T4, T5, T6, T7> builder() {
    return new ImmutableColumnHeaders7.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders7 ImmutableColumnHeaders7}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders7", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5, T6, T7> {
    private static final long INIT_BIT_HEADER7 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T7> header7;
    private ColumnHeaders6<T1, T2, T3, T4, T5, T6> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders7} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7> from(ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> instance) {
      Objects.requireNonNull(instance, "instance");
      header7(instance.header7());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders7#header7() header7} attribute.
     * @param header7 The value for header7 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7> header7(ColumnHeader<T7> header7) {
      this.header7 = Objects.requireNonNull(header7, "header7");
      initBits &= ~INIT_BIT_HEADER7;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders7#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6, T7> others(ColumnHeaders6<T1, T2, T3, T4, T5, T6> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders7 ImmutableColumnHeaders7}.
     * @return An immutable instance of ColumnHeaders7
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders7<>(null, header7, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER7) != 0) attributes.add("header7");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders7, some of required attributes are not set " + attributes;
    }
  }
}
