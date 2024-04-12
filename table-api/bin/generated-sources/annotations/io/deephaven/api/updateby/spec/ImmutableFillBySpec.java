package io.deephaven.api.updateby.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link FillBySpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableFillBySpec.builder()}.
 */
@Generated(from = "FillBySpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableFillBySpec extends FillBySpec {

  private ImmutableFillBySpec(ImmutableFillBySpec.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableFillBySpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableFillBySpec
        && equalTo(0, (ImmutableFillBySpec) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableFillBySpec another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 2049648457;
  }

  /**
   * Prints the immutable value {@code FillBySpec}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "FillBySpec{}";
  }

  /**
   * Creates an immutable copy of a {@link FillBySpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable FillBySpec instance
   */
  public static ImmutableFillBySpec copyOf(FillBySpec instance) {
    if (instance instanceof ImmutableFillBySpec) {
      return (ImmutableFillBySpec) instance;
    }
    return ImmutableFillBySpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableFillBySpec ImmutableFillBySpec}.
   * <pre>
   * ImmutableFillBySpec.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableFillBySpec builder
   */
  public static ImmutableFillBySpec.Builder builder() {
    return new ImmutableFillBySpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableFillBySpec ImmutableFillBySpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "FillBySpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code FillBySpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(FillBySpec instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableFillBySpec ImmutableFillBySpec}.
     * @return An immutable instance of FillBySpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableFillBySpec build() {
      return new ImmutableFillBySpec(this);
    }
  }
}
