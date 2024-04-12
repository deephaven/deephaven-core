package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedByteType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedByteType.builder()}.
 */
@Generated(from = "BoxedByteType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedByteType extends BoxedByteType {

  private ImmutableBoxedByteType(ImmutableBoxedByteType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedByteType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedByteType
        && equalTo(0, (ImmutableBoxedByteType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedByteType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1606993681;
  }

  /**
   * Creates an immutable copy of a {@link BoxedByteType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedByteType instance
   */
  public static ImmutableBoxedByteType copyOf(BoxedByteType instance) {
    if (instance instanceof ImmutableBoxedByteType) {
      return (ImmutableBoxedByteType) instance;
    }
    return ImmutableBoxedByteType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedByteType ImmutableBoxedByteType}.
   * <pre>
   * ImmutableBoxedByteType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedByteType builder
   */
  public static ImmutableBoxedByteType.Builder builder() {
    return new ImmutableBoxedByteType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedByteType ImmutableBoxedByteType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedByteType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedByteType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedByteType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedByteType ImmutableBoxedByteType}.
     * @return An immutable instance of BoxedByteType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedByteType build() {
      return new ImmutableBoxedByteType(this);
    }
  }
}
