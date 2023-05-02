package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CharType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCharType.builder()}.
 */
@Generated(from = "CharType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCharType extends CharType {

  private ImmutableCharType(ImmutableCharType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCharType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCharType
        && equalTo(0, (ImmutableCharType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableCharType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1906420395;
  }

  /**
   * Creates an immutable copy of a {@link CharType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CharType instance
   */
  public static ImmutableCharType copyOf(CharType instance) {
    if (instance instanceof ImmutableCharType) {
      return (ImmutableCharType) instance;
    }
    return ImmutableCharType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCharType ImmutableCharType}.
   * <pre>
   * ImmutableCharType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableCharType builder
   */
  public static ImmutableCharType.Builder builder() {
    return new ImmutableCharType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCharType ImmutableCharType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CharType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CharType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(CharType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableCharType ImmutableCharType}.
     * @return An immutable instance of CharType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCharType build() {
      return new ImmutableCharType(this);
    }
  }
}
