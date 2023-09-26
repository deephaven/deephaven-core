package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedCharType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedCharType.builder()}.
 */
@Generated(from = "BoxedCharType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedCharType extends BoxedCharType {

  private ImmutableBoxedCharType(ImmutableBoxedCharType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedCharType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedCharType
        && equalTo(0, (ImmutableBoxedCharType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedCharType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1696175165;
  }

  /**
   * Creates an immutable copy of a {@link BoxedCharType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedCharType instance
   */
  public static ImmutableBoxedCharType copyOf(BoxedCharType instance) {
    if (instance instanceof ImmutableBoxedCharType) {
      return (ImmutableBoxedCharType) instance;
    }
    return ImmutableBoxedCharType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedCharType ImmutableBoxedCharType}.
   * <pre>
   * ImmutableBoxedCharType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedCharType builder
   */
  public static ImmutableBoxedCharType.Builder builder() {
    return new ImmutableBoxedCharType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedCharType ImmutableBoxedCharType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedCharType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedCharType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedCharType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedCharType ImmutableBoxedCharType}.
     * @return An immutable instance of BoxedCharType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedCharType build() {
      return new ImmutableBoxedCharType(this);
    }
  }
}
