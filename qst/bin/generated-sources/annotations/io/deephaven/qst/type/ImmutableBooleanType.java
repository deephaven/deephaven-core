package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BooleanType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBooleanType.builder()}.
 */
@Generated(from = "BooleanType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableBooleanType extends BooleanType {

  private ImmutableBooleanType(ImmutableBooleanType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBooleanType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBooleanType
        && equalTo(0, (ImmutableBooleanType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableBooleanType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1945260219;
  }

  /**
   * Creates an immutable copy of a {@link BooleanType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BooleanType instance
   */
  public static ImmutableBooleanType copyOf(BooleanType instance) {
    if (instance instanceof ImmutableBooleanType) {
      return (ImmutableBooleanType) instance;
    }
    return ImmutableBooleanType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBooleanType ImmutableBooleanType}.
   * <pre>
   * ImmutableBooleanType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableBooleanType builder
   */
  public static ImmutableBooleanType.Builder builder() {
    return new ImmutableBooleanType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBooleanType ImmutableBooleanType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BooleanType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code BooleanType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(BooleanType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBooleanType ImmutableBooleanType}.
     * @return An immutable instance of BooleanType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBooleanType build() {
      return new ImmutableBooleanType(this);
    }
  }
}
