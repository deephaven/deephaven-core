package io.deephaven.api.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FilterNot}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFilterNot.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableFilterNot.of()}.
 */
@Generated(from = "FilterNot", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFilterNot extends FilterNot {
  private final Filter filter;

  private ImmutableFilterNot(Filter filter) {
    this.filter = Objects.requireNonNull(filter, "filter");
  }

  private ImmutableFilterNot(ImmutableFilterNot original, Filter filter) {
    this.filter = filter;
  }

  /**
   * The filter.
   * @return the filter
   */
  @Override
  public Filter filter() {
    return filter;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link FilterNot#filter() filter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for filter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableFilterNot withFilter(Filter value) {
    if (this.filter == value) return this;
    Filter newValue = Objects.requireNonNull(value, "filter");
    return new ImmutableFilterNot(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFilterNot} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFilterNot
        && equalTo(0, (ImmutableFilterNot) another);
  }

  private boolean equalTo(int synthetic, ImmutableFilterNot another) {
    return filter.equals(another.filter);
  }

  /**
   * Computes a hash code from attributes: {@code filter}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + filter.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code FilterNot} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FilterNot{"
        + "filter=" + filter
        + "}";
  }

  /**
   * Construct a new immutable {@code FilterNot} instance.
   * @param filter The value for the {@code filter} attribute
   * @return An immutable FilterNot instance
   */
  public static ImmutableFilterNot of(Filter filter) {
    return new ImmutableFilterNot(filter);
  }

  /**
   * Creates an immutable copy of a {@link FilterNot} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FilterNot instance
   */
  public static ImmutableFilterNot copyOf(FilterNot instance) {
    if (instance instanceof ImmutableFilterNot) {
      return (ImmutableFilterNot) instance;
    }
    return ImmutableFilterNot.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFilterNot ImmutableFilterNot}.
   * <pre>
   * ImmutableFilterNot.builder()
   *    .filter(io.deephaven.api.filter.Filter) // required {@link FilterNot#filter() filter}
   *    .build();
   * </pre>
   * @return A new ImmutableFilterNot builder
   */
  public static ImmutableFilterNot.Builder builder() {
    return new ImmutableFilterNot.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFilterNot ImmutableFilterNot}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FilterNot", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_FILTER = 0x1L;
    private long initBits = 0x1L;

    private @Nullable Filter filter;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FilterNot} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FilterNot instance) {
      Objects.requireNonNull(instance, "instance");
      filter(instance.filter());
      return this;
    }

    /**
     * Initializes the value for the {@link FilterNot#filter() filter} attribute.
     * @param filter The value for filter 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder filter(Filter filter) {
      this.filter = Objects.requireNonNull(filter, "filter");
      initBits &= ~INIT_BIT_FILTER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableFilterNot ImmutableFilterNot}.
     * @return An immutable instance of FilterNot
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFilterNot build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableFilterNot(null, filter);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FILTER) != 0) attributes.add("filter");
      return "Cannot build FilterNot, some of required attributes are not set " + attributes;
    }
  }
}
