package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders5}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders5.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders5.of()}.
 */
@Generated(from = "ColumnHeaders5", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders5<T1, T2, T3, T4, T5>
    extends ColumnHeaders5<T1, T2, T3, T4, T5> {
  private final ColumnHeader<T5> header5;
  private final ColumnHeaders4<T1, T2, T3, T4> others;

  private ImmutableColumnHeaders5(
      ColumnHeader<T5> header5,
      ColumnHeaders4<T1, T2, T3, T4> others) {
    this.header5 = Objects.requireNonNull(header5, "header5");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders5(
      ImmutableColumnHeaders5<T1, T2, T3, T4, T5> original,
      ColumnHeader<T5> header5,
      ColumnHeaders4<T1, T2, T3, T4> others) {
    this.header5 = header5;
    this.others = others;
  }

  /**
   * @return The value of the {@code header5} attribute
   */
  @Override
  public ColumnHeader<T5> header5() {
    return header5;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders4<T1, T2, T3, T4> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders5#header5() header5} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header5
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders5<T1, T2, T3, T4, T5> withHeader5(ColumnHeader<T5> value) {
    if (this.header5 == value) return this;
    ColumnHeader<T5> newValue = Objects.requireNonNull(value, "header5");
    return new ImmutableColumnHeaders5<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders5#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders5<T1, T2, T3, T4, T5> withOthers(ColumnHeaders4<T1, T2, T3, T4> value) {
    if (this.others == value) return this;
    ColumnHeaders4<T1, T2, T3, T4> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders5<>(this, this.header5, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders5} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders5<?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders5<?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders5<?, ?, ?, ?, ?> another) {
    return header5.equals(another.header5)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header5}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header5.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders5} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders5{"
        + "header5=" + header5
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders5} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
 * @param <T5> generic parameter T5
   * @param header5 The value for the {@code header5} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders5 instance
   */
  public static <T1, T2, T3, T4, T5> ImmutableColumnHeaders5<T1, T2, T3, T4, T5> of(ColumnHeader<T5> header5, ColumnHeaders4<T1, T2, T3, T4> others) {
    return new ImmutableColumnHeaders5<>(header5, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders5} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders5 instance
   */
  public static <T1, T2, T3, T4, T5> ImmutableColumnHeaders5<T1, T2, T3, T4, T5> copyOf(ColumnHeaders5<T1, T2, T3, T4, T5> instance) {
    if (instance instanceof ImmutableColumnHeaders5<?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeaders5<T1, T2, T3, T4, T5>) instance;
    }
    return ImmutableColumnHeaders5.<T1, T2, T3, T4, T5>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders5 ImmutableColumnHeaders5}.
   * <pre>
   * ImmutableColumnHeaders5.&amp;lt;T1, T2, T3, T4, T5&amp;gt;builder()
   *    .header5(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T5&amp;gt;) // required {@link ColumnHeaders5#header5() header5}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders4&amp;lt;T1, T2, T3, T4&amp;gt;) // required {@link ColumnHeaders5#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @return A new ImmutableColumnHeaders5 builder
   */
  public static <T1, T2, T3, T4, T5> ImmutableColumnHeaders5.Builder<T1, T2, T3, T4, T5> builder() {
    return new ImmutableColumnHeaders5.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders5 ImmutableColumnHeaders5}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders5", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5> {
    private static final long INIT_BIT_HEADER5 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T5> header5;
    private ColumnHeaders4<T1, T2, T3, T4> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders5} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5> from(ColumnHeaders5<T1, T2, T3, T4, T5> instance) {
      Objects.requireNonNull(instance, "instance");
      header5(instance.header5());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders5#header5() header5} attribute.
     * @param header5 The value for header5 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5> header5(ColumnHeader<T5> header5) {
      this.header5 = Objects.requireNonNull(header5, "header5");
      initBits &= ~INIT_BIT_HEADER5;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders5#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5> others(ColumnHeaders4<T1, T2, T3, T4> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders5 ImmutableColumnHeaders5}.
     * @return An immutable instance of ColumnHeaders5
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders5<T1, T2, T3, T4, T5> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders5<>(null, header5, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER5) != 0) attributes.add("header5");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders5, some of required attributes are not set " + attributes;
    }
  }
}
