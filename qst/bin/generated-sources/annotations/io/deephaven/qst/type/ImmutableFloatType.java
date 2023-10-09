package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FloatType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFloatType.builder()}.
 */
@Generated(from = "FloatType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableFloatType extends FloatType {

  private ImmutableFloatType(ImmutableFloatType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFloatType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFloatType
        && equalTo(0, (ImmutableFloatType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableFloatType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -399719303;
  }

  /**
   * Creates an immutable copy of a {@link FloatType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FloatType instance
   */
  public static ImmutableFloatType copyOf(FloatType instance) {
    if (instance instanceof ImmutableFloatType) {
      return (ImmutableFloatType) instance;
    }
    return ImmutableFloatType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFloatType ImmutableFloatType}.
   * <pre>
   * ImmutableFloatType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableFloatType builder
   */
  public static ImmutableFloatType.Builder builder() {
    return new ImmutableFloatType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFloatType ImmutableFloatType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FloatType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FloatType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FloatType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableFloatType ImmutableFloatType}.
     * @return An immutable instance of FloatType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFloatType build() {
      return new ImmutableFloatType(this);
    }
  }
}
