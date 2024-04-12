package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders3}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders3.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders3.of()}.
 */
@Generated(from = "ColumnHeaders3", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders3<T1, T2, T3>
    extends ColumnHeaders3<T1, T2, T3> {
  private final ColumnHeader<T3> header3;
  private final ColumnHeaders2<T1, T2> others;

  private ImmutableColumnHeaders3(
      ColumnHeader<T3> header3,
      ColumnHeaders2<T1, T2> others) {
    this.header3 = Objects.requireNonNull(header3, "header3");
    this.others = Objects.requireNonNull(others, "others");
  }

  private ImmutableColumnHeaders3(
      ImmutableColumnHeaders3<T1, T2, T3> original,
      ColumnHeader<T3> header3,
      ColumnHeaders2<T1, T2> others) {
    this.header3 = header3;
    this.others = others;
  }

  /**
   * @return The value of the {@code header3} attribute
   */
  @Override
  public ColumnHeader<T3> header3() {
    return header3;
  }

  /**
   * @return The value of the {@code others} attribute
   */
  @Override
  public ColumnHeaders2<T1, T2> others() {
    return others;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders3#header3() header3} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header3
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders3<T1, T2, T3> withHeader3(ColumnHeader<T3> value) {
    if (this.header3 == value) return this;
    ColumnHeader<T3> newValue = Objects.requireNonNull(value, "header3");
    return new ImmutableColumnHeaders3<>(this, newValue, this.others);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders3#others() others} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for others
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders3<T1, T2, T3> withOthers(ColumnHeaders2<T1, T2> value) {
    if (this.others == value) return this;
    ColumnHeaders2<T1, T2> newValue = Objects.requireNonNull(value, "others");
    return new ImmutableColumnHeaders3<>(this, this.header3, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders3} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders3<?, ?, ?>
        && equalTo(0, (ImmutableColumnHeaders3<?, ?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders3<?, ?, ?> another) {
    return header3.equals(another.header3)
        && others.equals(another.others);
  }

  /**
   * Computes a hash code from attributes: {@code header3}, {@code others}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header3.hashCode();
    h += (h << 5) + others.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders3} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders3{"
        + "header3=" + header3
        + ", others=" + others
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders3} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
 * @param <T3> generic parameter T3
   * @param header3 The value for the {@code header3} attribute
   * @param others The value for the {@code others} attribute
   * @return An immutable ColumnHeaders3 instance
   */
  public static <T1, T2, T3> ImmutableColumnHeaders3<T1, T2, T3> of(ColumnHeader<T3> header3, ColumnHeaders2<T1, T2> others) {
    return new ImmutableColumnHeaders3<>(header3, others);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders3} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders3 instance
   */
  public static <T1, T2, T3> ImmutableColumnHeaders3<T1, T2, T3> copyOf(ColumnHeaders3<T1, T2, T3> instance) {
    if (instance instanceof ImmutableColumnHeaders3<?, ?, ?>) {
      return (ImmutableColumnHeaders3<T1, T2, T3>) instance;
    }
    return ImmutableColumnHeaders3.<T1, T2, T3>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders3 ImmutableColumnHeaders3}.
   * <pre>
   * ImmutableColumnHeaders3.&amp;lt;T1, T2, T3&amp;gt;builder()
   *    .header3(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T3&amp;gt;) // required {@link ColumnHeaders3#header3() header3}
   *    .others(io.deephaven.qst.column.header.ColumnHeaders2&amp;lt;T1, T2&amp;gt;) // required {@link ColumnHeaders3#others() others}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param <T3> generic parameter T3
   * @return A new ImmutableColumnHeaders3 builder
   */
  public static <T1, T2, T3> ImmutableColumnHeaders3.Builder<T1, T2, T3> builder() {
    return new ImmutableColumnHeaders3.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders3 ImmutableColumnHeaders3}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders3", generator = "Immutables")
  public static final class Builder<T1, T2, T3> {
    private static final long INIT_BIT_HEADER3 = 0x1L;
    private static final long INIT_BIT_OTHERS = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T3> header3;
    private ColumnHeaders2<T1, T2> others;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders3} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3> from(ColumnHeaders3<T1, T2, T3> instance) {
      Objects.requireNonNull(instance, "instance");
      header3(instance.header3());
      others(instance.others());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders3#header3() header3} attribute.
     * @param header3 The value for header3 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3> header3(ColumnHeader<T3> header3) {
      this.header3 = Objects.requireNonNull(header3, "header3");
      initBits &= ~INIT_BIT_HEADER3;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders3#others() others} attribute.
     * @param others The value for others 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2, T3> others(ColumnHeaders2<T1, T2> others) {
      this.others = Objects.requireNonNull(others, "others");
      initBits &= ~INIT_BIT_OTHERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders3 ImmutableColumnHeaders3}.
     * @return An immutable instance of ColumnHeaders3
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders3<T1, T2, T3> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders3<>(null, header3, others);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER3) != 0) attributes.add("header3");
      if ((initBits & INIT_BIT_OTHERS) != 0) attributes.add("others");
      return "Cannot build ColumnHeaders3, some of required attributes are not set " + attributes;
    }
  }
}
