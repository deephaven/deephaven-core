package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedShortType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedShortType.builder()}.
 */
@Generated(from = "BoxedShortType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedShortType extends BoxedShortType {

  private ImmutableBoxedShortType(ImmutableBoxedShortType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedShortType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedShortType
        && equalTo(0, (ImmutableBoxedShortType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedShortType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1755930439;
  }

  /**
   * Creates an immutable copy of a {@link BoxedShortType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedShortType instance
   */
  public static ImmutableBoxedShortType copyOf(BoxedShortType instance) {
    if (instance instanceof ImmutableBoxedShortType) {
      return (ImmutableBoxedShortType) instance;
    }
    return ImmutableBoxedShortType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedShortType ImmutableBoxedShortType}.
   * <pre>
   * ImmutableBoxedShortType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedShortType builder
   */
  public static ImmutableBoxedShortType.Builder builder() {
    return new ImmutableBoxedShortType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedShortType ImmutableBoxedShortType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedShortType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedShortType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedShortType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedShortType ImmutableBoxedShortType}.
     * @return An immutable instance of BoxedShortType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedShortType build() {
      return new ImmutableBoxedShortType(this);
    }
  }
}
