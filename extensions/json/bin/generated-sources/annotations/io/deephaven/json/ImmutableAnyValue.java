package io.deephaven.json;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AnyValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAnyValue.builder()}.
 */
@Generated(from = "AnyValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAnyValue extends AnyValue {

  private ImmutableAnyValue(ImmutableAnyValue.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAnyValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAnyValue
        && equalTo(0, (ImmutableAnyValue) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAnyValue another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 1890474062;
  }

  /**
   * Prints the immutable value {@code AnyValue}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AnyValue{}";
  }

  private static ImmutableAnyValue validate(ImmutableAnyValue instance) {
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AnyValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AnyValue instance
   */
  public static ImmutableAnyValue copyOf(AnyValue instance) {
    if (instance instanceof ImmutableAnyValue) {
      return (ImmutableAnyValue) instance;
    }
    return ImmutableAnyValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAnyValue ImmutableAnyValue}.
   * <pre>
   * ImmutableAnyValue.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAnyValue builder
   */
  public static ImmutableAnyValue.Builder builder() {
    return new ImmutableAnyValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAnyValue ImmutableAnyValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AnyValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AnyValue} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AnyValue instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAnyValue ImmutableAnyValue}.
     * @return An immutable instance of AnyValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAnyValue build() {
      return ImmutableAnyValue.validate(new ImmutableAnyValue(this));
    }
  }
}
