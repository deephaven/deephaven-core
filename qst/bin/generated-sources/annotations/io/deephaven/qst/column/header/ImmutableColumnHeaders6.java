package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders6}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders6.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders6.of()}.
 */
@Generated(from = "ColumnHeaders6", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6>
    extends ColumnHeaders6<T1, T2, T3, T4, T5, T6> {
  private final ColumnHeader<T6> header6;
  private final ColumnHeaders5<T1, T2, T3, T4, T5> others;

  private ImmutableColumnHeaders6(
      ColumnHeader<T6> header6,
      ColumnHeaders5<T1, T2, T3, T4, T5> others) {
    this.header6 = Objects.requireNonNull(header6, "header6");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders6(
      ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> original,
      ColumnHeader<T6> header6,
      ColumnHeaders5<T1, T2, T3, T4, T5> others) {
    this.header6 = header6;
    this.others = others;
  }

  /**
   * @return The value of the {@code header6} attribute
   */
  @Override
  public ColumnHeader<T6> header6() {
    return header6;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders5<T1, T2, T3, T4, T5> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders6#header6() header6} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header6
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> withHeader6(ColumnHeader<T6> value) {
    if (this.header6 == value) return this;
    ColumnHeader<T6> newValue = Objects.requireNonNull(value, "header6");
    return new ImmutableColumnHeaders6<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders6#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> withOthers(ColumnHeaders5<T1, T2, T3, T4, T5> value) {
    if (this.others == value) return this;
    ColumnHeaders5<T1, T2, T3, T4, T5> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders6<>(this, this.header6, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders6} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders6<?, ?, ?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders6<?, ?, ?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders6<?, ?, ?, ?, ?, ?> another) {
    return header6.equals(another.header6)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header6}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header6.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders6} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders6{"
        + "header6=" + header6
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders6} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
 * @param <T5> generic parameter T5
 * @param <T6> generic parameter T6
   * @param header6 The value for the {@code header6} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders6 instance
   */
  public static <T1, T2, T3, T4, T5, T6> ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> of(ColumnHeader<T6> header6, ColumnHeaders5<T1, T2, T3, T4, T5> others) {
    return new ImmutableColumnHeaders6<>(header6, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders6} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders6 instance
   */
  public static <T1, T2, T3, T4, T5, T6> ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> copyOf(ColumnHeaders6<T1, T2, T3, T4, T5, T6> instance) {
    if (instance instanceof ImmutableColumnHeaders6<?, ?, ?, ?, ?, ?>) {
      return (ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6>) instance;
    }
    return ImmutableColumnHeaders6.<T1, T2, T3, T4, T5, T6>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders6 ImmutableColumnHeaders6}.
   * <pre>
   * ImmutableColumnHeaders6.&amp;lt;T1, T2, T3, T4, T5, T6&amp;gt;builder()
   *    .header6(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T6&amp;gt;) // required {@link ColumnHeaders6#header6() header6}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders5&amp;lt;T1, T2, T3, T4, T5&amp;gt;) // required {@link ColumnHeaders6#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param <T5> generic parameter T5
   * @param <T6> generic parameter T6
   * @return A new ImmutableColumnHeaders6 builder
   */
  public static <T1, T2, T3, T4, T5, T6> ImmutableColumnHeaders6.Builder<T1, T2, T3, T4, T5, T6> builder() {
    return new ImmutableColumnHeaders6.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders6 ImmutableColumnHeaders6}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders6", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4, T5, T6> {
    private static final long INIT_BIT_HEADER6 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T6> header6;
    private ColumnHeaders5<T1, T2, T3, T4, T5> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders6} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6> from(ColumnHeaders6<T1, T2, T3, T4, T5, T6> instance) {
      Objects.requireNonNull(instance, "instance");
      header6(instance.header6());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders6#header6() header6} attribute.
     * @param header6 The value for header6 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6> header6(ColumnHeader<T6> header6) {
      this.header6 = Objects.requireNonNull(header6, "header6");
      initBits &= ~INIT_BIT_HEADER6;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders6#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4, T5, T6> others(ColumnHeaders5<T1, T2, T3, T4, T5> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders6 ImmutableColumnHeaders6}.
     * @return An immutable instance of ColumnHeaders6
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders6<T1, T2, T3, T4, T5, T6> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders6<>(null, header6, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER6) != 0) attributes.add("header6");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders6, some of required attributes are not set " + attributes;
    }
  }
}
