package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link StringType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableStringType.builder()}.
 */
@Generated(from = "StringType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableStringType extends StringType {

  private ImmutableStringType(ImmutableStringType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableStringType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableStringType
        && equalTo(0, (ImmutableStringType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableStringType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -52602138;
  }

  /**
   * Creates an immutable copy of a {@link StringType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable StringType instance
   */
  public static ImmutableStringType copyOf(StringType instance) {
    if (instance instanceof ImmutableStringType) {
      return (ImmutableStringType) instance;
    }
    return ImmutableStringType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableStringType ImmutableStringType}.
   * <pre>
   * ImmutableStringType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableStringType builder
   */
  public static ImmutableStringType.Builder builder() {
    return new ImmutableStringType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableStringType ImmutableStringType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "StringType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code StringType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(StringType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableStringType ImmutableStringType}.
     * @return An immutable instance of StringType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableStringType build() {
      return new ImmutableStringType(this);
    }
  }
}
