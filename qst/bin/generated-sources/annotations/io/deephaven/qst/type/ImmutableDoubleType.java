package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DoubleType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDoubleType.builder()}.
 */
@Generated(from = "DoubleType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableDoubleType extends DoubleType {

  private ImmutableDoubleType(ImmutableDoubleType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDoubleType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDoubleType
        && equalTo(0, (ImmutableDoubleType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableDoubleType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return -1995729242;
  }

  /**
   * Creates an immutable copy of a {@link DoubleType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DoubleType instance
   */
  public static ImmutableDoubleType copyOf(DoubleType instance) {
    if (instance instanceof ImmutableDoubleType) {
      return (ImmutableDoubleType) instance;
    }
    return ImmutableDoubleType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDoubleType ImmutableDoubleType}.
   * <pre>
   * ImmutableDoubleType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableDoubleType builder
   */
  public static ImmutableDoubleType.Builder builder() {
    return new ImmutableDoubleType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDoubleType ImmutableDoubleType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DoubleType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DoubleType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(DoubleType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableDoubleType ImmutableDoubleType}.
     * @return An immutable instance of DoubleType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDoubleType build() {
      return new ImmutableDoubleType(this);
    }
  }
}
