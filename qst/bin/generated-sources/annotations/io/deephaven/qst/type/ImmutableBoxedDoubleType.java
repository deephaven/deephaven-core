package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedDoubleType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedDoubleType.builder()}.
 */
@Generated(from = "BoxedDoubleType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedDoubleType extends BoxedDoubleType {

  private ImmutableBoxedDoubleType(ImmutableBoxedDoubleType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedDoubleType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedDoubleType
        && equalTo(0, (ImmutableBoxedDoubleType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedDoubleType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 2117034936;
  }

  /**
   * Creates an immutable copy of a {@link BoxedDoubleType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedDoubleType instance
   */
  public static ImmutableBoxedDoubleType copyOf(BoxedDoubleType instance) {
    if (instance instanceof ImmutableBoxedDoubleType) {
      return (ImmutableBoxedDoubleType) instance;
    }
    return ImmutableBoxedDoubleType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedDoubleType ImmutableBoxedDoubleType}.
   * <pre>
   * ImmutableBoxedDoubleType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedDoubleType builder
   */
  public static ImmutableBoxedDoubleType.Builder builder() {
    return new ImmutableBoxedDoubleType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedDoubleType ImmutableBoxedDoubleType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedDoubleType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedDoubleType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedDoubleType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedDoubleType ImmutableBoxedDoubleType}.
     * @return An immutable instance of BoxedDoubleType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedDoubleType build() {
      return new ImmutableBoxedDoubleType(this);
    }
  }
}
