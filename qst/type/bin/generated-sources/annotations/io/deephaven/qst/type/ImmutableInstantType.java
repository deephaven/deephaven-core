package io.deephaven.qst.type;

import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link InstantType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableInstantType.builder()}.
 */
@Generated(from = "InstantType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableInstantType extends InstantType {

  private ImmutableInstantType(ImmutableInstantType.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableInstantType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableInstantType
        && equalTo(0, (ImmutableInstantType) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableInstantType another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 2097865854;
  }

  /**
   * Creates an immutable copy of a {@link InstantType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable InstantType instance
   */
  public static ImmutableInstantType copyOf(InstantType instance) {
    if (instance instanceof ImmutableInstantType) {
      return (ImmutableInstantType) instance;
    }
    return ImmutableInstantType.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableInstantType ImmutableInstantType}.
   * <pre>
   * ImmutableInstantType.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableInstantType builder
   */
  public static ImmutableInstantType.Builder builder() {
    return new ImmutableInstantType.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableInstantType ImmutableInstantType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "InstantType", generator = "Immutables")
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code InstantType} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(InstantType instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableInstantType ImmutableInstantType}.
     * @return An immutable instance of InstantType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableInstantType build() {
      return new ImmutableInstantType(this);
    }
  }
}
