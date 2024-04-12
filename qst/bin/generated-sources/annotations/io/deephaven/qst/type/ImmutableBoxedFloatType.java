package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedFloatType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedFloatType.builder()}.
 */
@Generated(from = "BoxedFloatType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedFloatType extends BoxedFloatType {

  private ImmutableBoxedFloatType(ImmutableBoxedFloatType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedFloatType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedFloatType
        && equalTo(0, (ImmutableBoxedFloatType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedFloatType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1672613159;
  }

  /**
   * Creates an immutable copy of a {@link BoxedFloatType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedFloatType instance
   */
  public static ImmutableBoxedFloatType copyOf(BoxedFloatType instance) {
    if (instance instanceof ImmutableBoxedFloatType) {
      return (ImmutableBoxedFloatType) instance;
    }
    return ImmutableBoxedFloatType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedFloatType ImmutableBoxedFloatType}.
   * <pre>
   * ImmutableBoxedFloatType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedFloatType builder
   */
  public static ImmutableBoxedFloatType.Builder builder() {
    return new ImmutableBoxedFloatType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedFloatType ImmutableBoxedFloatType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedFloatType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedFloatType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedFloatType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedFloatType ImmutableBoxedFloatType}.
     * @return An immutable instance of BoxedFloatType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedFloatType build() {
      return new ImmutableBoxedFloatType(this);
    }
  }
}
