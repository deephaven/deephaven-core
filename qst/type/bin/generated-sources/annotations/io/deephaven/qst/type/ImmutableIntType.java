package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link IntType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIntType.builder()}.
 */
@Generated(from = "IntType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableIntType extends IntType {

  private ImmutableIntType(ImmutableIntType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIntType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIntType
        && equalTo(0, (ImmutableIntType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableIntType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1768025716;
  }

  /**
   * Creates an immutable copy of a {@link IntType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IntType instance
   */
  public static ImmutableIntType copyOf(IntType instance) {
    if (instance instanceof ImmutableIntType) {
      return (ImmutableIntType) instance;
    }
    return ImmutableIntType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIntType ImmutableIntType}.
   * <pre>
   * ImmutableIntType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableIntType builder
   */
  public static ImmutableIntType.Builder builder() {
    return new ImmutableIntType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIntType ImmutableIntType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IntType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IntType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(IntType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableIntType ImmutableIntType}.
     * @return An immutable instance of IntType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIntType build() {
      return new ImmutableIntType(this);
    }
  }
}
