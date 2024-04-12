package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BoxedBooleanType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBoxedBooleanType.builder()}.
 */
@Generated(from = "BoxedBooleanType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBoxedBooleanType extends BoxedBooleanType {

  private ImmutableBoxedBooleanType(ImmutableBoxedBooleanType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBoxedBooleanType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBoxedBooleanType
        && equalTo(0, (ImmutableBoxedBooleanType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBoxedBooleanType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 996377715;
  }

  /**
   * Creates an immutable copy of a {@link BoxedBooleanType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BoxedBooleanType instance
   */
  public static ImmutableBoxedBooleanType copyOf(BoxedBooleanType instance) {
    if (instance instanceof ImmutableBoxedBooleanType) {
      return (ImmutableBoxedBooleanType) instance;
    }
    return ImmutableBoxedBooleanType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBoxedBooleanType ImmutableBoxedBooleanType}.
   * <pre>
   * ImmutableBoxedBooleanType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBoxedBooleanType builder
   */
  public static ImmutableBoxedBooleanType.Builder builder() {
    return new ImmutableBoxedBooleanType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBoxedBooleanType ImmutableBoxedBooleanType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BoxedBooleanType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BoxedBooleanType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BoxedBooleanType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBoxedBooleanType ImmutableBoxedBooleanType}.
     * @return An immutable instance of BoxedBooleanType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBoxedBooleanType build() {
      return new ImmutableBoxedBooleanType(this);
    }
  }
}
