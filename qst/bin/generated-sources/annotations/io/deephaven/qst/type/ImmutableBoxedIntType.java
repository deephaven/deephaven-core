package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedIntType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedIntType.builder()}.
 */
@Generated(from = "BoxedIntType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedIntType extends BoxedIntType {

  private ImmutableBoxedIntType(ImmutableBoxedIntType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedIntType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedIntType
        && equalTo(0, (ImmutableBoxedIntType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedIntType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 580496826;
  }

  /**
   * Creates an immutable copy of a {@link BoxedIntType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedIntType instance
   */
  public static ImmutableBoxedIntType copyOf(BoxedIntType instance) {
    if (instance instanceof ImmutableBoxedIntType) {
      return (ImmutableBoxedIntType) instance;
    }
    return ImmutableBoxedIntType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedIntType ImmutableBoxedIntType}.
   * <pre>
   * ImmutableBoxedIntType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedIntType builder
   */
  public static ImmutableBoxedIntType.Builder builder() {
    return new ImmutableBoxedIntType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedIntType ImmutableBoxedIntType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedIntType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedIntType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedIntType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedIntType ImmutableBoxedIntType}.
     * @return An immutable instance of BoxedIntType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedIntType build() {
      return new ImmutableBoxedIntType(this);
    }
  }
}
