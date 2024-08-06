package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedLongType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedLongType.builder()}.
 */
@Generated(from = "BoxedLongType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedLongType extends BoxedLongType {

  private ImmutableBoxedLongType(ImmutableBoxedLongType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedLongType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedLongType
        && equalTo(0, (ImmutableBoxedLongType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedLongType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1813789597;
  }

  /**
   * Creates an immutable copy of a {@link BoxedLongType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedLongType instance
   */
  public static ImmutableBoxedLongType copyOf(BoxedLongType instance) {
    if (instance instanceof ImmutableBoxedLongType) {
      return (ImmutableBoxedLongType) instance;
    }
    return ImmutableBoxedLongType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedLongType ImmutableBoxedLongType}.
   * <pre>
   * ImmutableBoxedLongType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedLongType builder
   */
  public static ImmutableBoxedLongType.Builder builder() {
    return new ImmutableBoxedLongType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedLongType ImmutableBoxedLongType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedLongType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedLongType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedLongType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedLongType ImmutableBoxedLongType}.
     * @return An immutable instance of BoxedLongType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedLongType build() {
      return new ImmutableBoxedLongType(this);
    }
  }
}
