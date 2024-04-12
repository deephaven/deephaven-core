package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders4}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders4.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders4.of()}.
 */
@Generated(from = "ColumnHeaders4", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders4<T1, T2, T3, T4>
    extends ColumnHeaders4<T1, T2, T3, T4> {
  private final ColumnHeader<T4> header4;
  private final ColumnHeaders3<T1, T2, T3> others;

  private ImmutableColumnHeaders4(
      ColumnHeader<T4> header4,
      ColumnHeaders3<T1, T2, T3> others) {
    this.header4 = Objects.requireNonNull(header4, "header4");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders4(
      ImmutableColumnHeaders4<T1, T2, T3, T4> original,
      ColumnHeader<T4> header4,
      ColumnHeaders3<T1, T2, T3> others) {
    this.header4 = header4;
    this.others = others;
  }

  /**
   * @return The value of the {@code header4} attribute
   */
  @Override
  public ColumnHeader<T4> header4() {
    return header4;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders3<T1, T2, T3> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders4#header4() header4} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header4
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders4<T1, T2, T3, T4> withHeader4(ColumnHeader<T4> value) {
    if (this.header4 == value) return this;
    ColumnHeader<T4> newValue = Objects.requireNonNull(value, "header4");
    return new ImmutableColumnHeaders4<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders4#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders4<T1, T2, T3, T4> withOthers(ColumnHeaders3<T1, T2, T3> value) {
    if (this.others == value) return this;
    ColumnHeaders3<T1, T2, T3> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders4<>(this, this.header4, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders4} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders4<?, ?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders4<?, ?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders4<?, ?, ?, ?> another) {
    return header4.equals(another.header4)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header4}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header4.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders4} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders4{"
        + "header4=" + header4
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders4} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
 * @param <T4> generic parameter T4
   * @param header4 The value for the {@code header4} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders4 instance
   */
  public static <T1, T2, T3, T4> ImmutableColumnHeaders4<T1, T2, T3, T4> of(ColumnHeader<T4> header4, ColumnHeaders3<T1, T2, T3> others) {
    return new ImmutableColumnHeaders4<>(header4, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders4} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders4 instance
   */
  public static <T1, T2, T3, T4> ImmutableColumnHeaders4<T1, T2, T3, T4> copyOf(ColumnHeaders4<T1, T2, T3, T4> instance) {
    if (instance instanceof ImmutableColumnHeaders4<?, ?, ?, ?>) {
      return (ImmutableColumnHeaders4<T1, T2, T3, T4>) instance;
    }
    return ImmutableColumnHeaders4.<T1, T2, T3, T4>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders4 ImmutableColumnHeaders4}.
   * <pre>
   * ImmutableColumnHeaders4.&amp;lt;T1, T2, T3, T4&amp;gt;builder()
   *    .header4(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T4&amp;gt;) // required {@link ColumnHeaders4#header4() header4}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders3&amp;lt;T1, T2, T3&amp;gt;) // required {@link ColumnHeaders4#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param <T4> generic parameter T4
   * @return A new ImmutableColumnHeaders4 builder
   */
  public static <T1, T2, T3, T4> ImmutableColumnHeaders4.Builder<T1, T2, T3, T4> builder() {
    return new ImmutableColumnHeaders4.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders4 ImmutableColumnHeaders4}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders4", generator = "Immutables")
  public static final class Builder<T1, T2, T3, T4> {
    private static final long INIT_BIT_HEADER4 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T4> header4;
    private ColumnHeaders3<T1, T2, T3> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders4} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4> from(ColumnHeaders4<T1, T2, T3, T4> instance) {
      Objects.requireNonNull(instance, "instance");
      header4(instance.header4());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders4#header4() header4} attribute.
     * @param header4 The value for header4 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4> header4(ColumnHeader<T4> header4) {
      this.header4 = Objects.requireNonNull(header4, "header4");
      initBits &= ~INIT_BIT_HEADER4;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders4#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3, T4> others(ColumnHeaders3<T1, T2, T3> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders4 ImmutableColumnHeaders4}.
     * @return An immutable instance of ColumnHeaders4
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders4<T1, T2, T3, T4> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders4<>(null, header4, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER4) != 0) attributes.add("header4");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders4, some of required attributes are not set " + attributes;
    }
  }
}
