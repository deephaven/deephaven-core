package io.deephaven.qst.column.header;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnHeaders2}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnHeaders2.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnHeaders2.of()}.
 */
@Generated(from = "ColumnHeaders2", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableColumnHeaders2<T1, T2>
    extends ColumnHeaders2<T1, T2> {
  private final ColumnHeader<T1> header1;
  private final ColumnHeader<T2> header2;

  private ImmutableColumnHeaders2(
      ColumnHeader<T1> header1,
      ColumnHeader<T2> header2) {
    this.header1 = Objects.requireNonNull(header1, "header1");
    this.header2 = Objects.requireNonNull(header2, "header2");
  }

  private ImmutableColumnHeaders2(
      ImmutableColumnHeaders2<T1, T2> original,
      ColumnHeader<T1> header1,
      ColumnHeader<T2> header2) {
    this.header1 = header1;
    this.header2 = header2;
  }

  /**
   * @return The value of the {@code header1} attribute
   */
  @Override
  public ColumnHeader<T1> header1() {
    return header1;
  }

  /**
   * @return The value of the {@code header2} attribute
   */
  @Override
  public ColumnHeader<T2> header2() {
    return header2;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders2#header1() header1} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header1
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders2<T1, T2> withHeader1(ColumnHeader<T1> value) {
    if (this.header1 == value) return this;
    ColumnHeader<T1> newValue = Objects.requireNonNull(value, "header1");
    return new ImmutableColumnHeaders2<>(this, newValue, this.header2);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnHeaders2#header2() header2} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for header2
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnHeaders2<T1, T2> withHeader2(ColumnHeader<T2> value) {
    if (this.header2 == value) return this;
    ColumnHeader<T2> newValue = Objects.requireNonNull(value, "header2");
    return new ImmutableColumnHeaders2<>(this, this.header1, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnHeaders2} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnHeaders2<?, ?>
        && equalTo(0, (ImmutableColumnHeaders2<?, ?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnHeaders2<?, ?> another) {
    return header1.equals(another.header1)
        && header2.equals(another.header2);
  }

  /**
   * Computes a hash code from attributes: {@code header1}, {@code header2}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + header1.hashCode();
    h += (h << 5) + header2.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ColumnHeaders2} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ColumnHeaders2{"
        + "header1=" + header1
        + ", header2=" + header2
        + "}";
  }

  /**
   * Construct a new immutable {@code ColumnHeaders2} instance.
 * @param <T1> generic parameter T1
 * @param <T2> generic parameter T2
   * @param header1 The value for the {@code header1} attribute
   * @param header2 The value for the {@code header2} attribute
   * @return An immutable ColumnHeaders2 instance
   */
  public static <T1, T2> ImmutableColumnHeaders2<T1, T2> of(ColumnHeader<T1> header1, ColumnHeader<T2> header2) {
    return new ImmutableColumnHeaders2<>(header1, header2);
  }

  /**
   * Creates an immutable copy of a {@link ColumnHeaders2} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @param instance The instance to copy
   * @return A copied immutable ColumnHeaders2 instance
   */
  public static <T1, T2> ImmutableColumnHeaders2<T1, T2> copyOf(ColumnHeaders2<T1, T2> instance) {
    if (instance instanceof ImmutableColumnHeaders2<?, ?>) {
      return (ImmutableColumnHeaders2<T1, T2>) instance;
    }
    return ImmutableColumnHeaders2.<T1, T2>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnHeaders2 ImmutableColumnHeaders2}.
   * <pre>
   * ImmutableColumnHeaders2.&amp;lt;T1, T2&amp;gt;builder()
   *    .header1(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T1&amp;gt;) // required {@link ColumnHeaders2#header1() header1}
   *    .header2(io.deephaven.qst.column.header.ColumnHeader&amp;lt;T2&amp;gt;) // required {@link ColumnHeaders2#header2() header2}
   *    .build();
   * </pre>
   * @param <T1> generic parameter T1
   * @param <T2> generic parameter T2
   * @return A new ImmutableColumnHeaders2 builder
   */
  public static <T1, T2> ImmutableColumnHeaders2.Builder<T1, T2> builder() {
    return new ImmutableColumnHeaders2.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableColumnHeaders2 ImmutableColumnHeaders2}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnHeaders2", generator = "Immutables")
  public static final class Builder<T1, T2> {
    private static final long INIT_BIT_HEADER1 = 0x1L;
    private static final long INIT_BIT_HEADER2 = 0x2L;
    private long initBits = 0x3L;

    private ColumnHeader<T1> header1;
    private ColumnHeader<T2> header2;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnHeaders2} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2> from(ColumnHeaders2<T1, T2> instance) {
      Objects.requireNonNull(instance, "instance");
      header1(instance.header1());
      header2(instance.header2());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders2#header1() header1} attribute.
     * @param header1 The value for header1 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2> header1(ColumnHeader<T1> header1) {
      this.header1 = Objects.requireNonNull(header1, "header1");
      initBits &= ~INIT_BIT_HEADER1;
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnHeaders2#header2() header2} attribute.
     * @param header2 The value for header2 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T1, T2> header2(ColumnHeader<T2> header2) {
      this.header2 = Objects.requireNonNull(header2, "header2");
      initBits &= ~INIT_BIT_HEADER2;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnHeaders2 ImmutableColumnHeaders2}.
     * @return An immutable instance of ColumnHeaders2
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnHeaders2<T1, T2> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableColumnHeaders2<>(null, header1, header2);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_HEADER1) != 0) attributes.add("header1");
      if ((initBits & INIT_BIT_HEADER2) != 0) attributes.add("header2");
      return "Cannot build ColumnHeaders2, some of required attributes are not set " + attributes;
    }
  }
}
