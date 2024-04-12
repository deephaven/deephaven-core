package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LongType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLongType.builder()}.
 */
@Generated(from = "LongType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableLongType extends LongType {

  private ImmutableLongType(ImmutableLongType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLongType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLongType
        && equalTo(0, (ImmutableLongType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableLongType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1603544367;
  }

  /**
   * Creates an immutable copy of a {@link LongType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LongType instance
   */
  public static ImmutableLongType copyOf(LongType instance) {
    if (instance instanceof ImmutableLongType) {
      return (ImmutableLongType) instance;
    }
    return ImmutableLongType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLongType ImmutableLongType}.
   * <pre>
   * ImmutableLongType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableLongType builder
   */
  public static ImmutableLongType.Builder builder() {
    return new ImmutableLongType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLongType ImmutableLongType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LongType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LongType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LongType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableLongType ImmutableLongType}.
     * @return An immutable instance of LongType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLongType build() {
      return new ImmutableLongType(this);
    }
  }
}
