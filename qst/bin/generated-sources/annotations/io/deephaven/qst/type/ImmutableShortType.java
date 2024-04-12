package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ShortType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableShortType.builder()}.
 */
@Generated(from = "ShortType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableShortType extends ShortType {

  private ImmutableShortType(ImmutableShortType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableShortType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableShortType
        && equalTo(0, (ImmutableShortType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableShortType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -316402023;
  }

  /**
   * Creates an immutable copy of a {@link ShortType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ShortType instance
   */
  public static ImmutableShortType copyOf(ShortType instance) {
    if (instance instanceof ImmutableShortType) {
      return (ImmutableShortType) instance;
    }
    return ImmutableShortType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableShortType ImmutableShortType}.
   * <pre>
   * ImmutableShortType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableShortType builder
   */
  public static ImmutableShortType.Builder builder() {
    return new ImmutableShortType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableShortType ImmutableShortType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ShortType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ShortType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ShortType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableShortType ImmutableShortType}.
     * @return An immutable instance of ShortType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableShortType build() {
      return new ImmutableShortType(this);
    }
  }
}
