package io.deephaven.api.agg.spec;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecVar}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecVar.builder()}.
 */
@Generated(from = "AggSpecVar", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecVar extends AggSpecVar {

  private ImmutableAggSpecVar(ImmutableAggSpecVar.Builder builder) {
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecVar} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecVar
        && equalTo(0, (ImmutableAggSpecVar) another);
  }

  @SuppressWarnings("MethodCanBeStatic")
  private boolean equalTo(int synthetic, ImmutableAggSpecVar another) {
    return true;
  }

  /**
   * Returns a constant hash code value.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return 493345440;
  }

  /**
   * Prints the immutable value {@code AggSpecVar}.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "AggSpecVar{}";
  }

  /**
   * Creates an immutable copy of a {@link AggSpecVar} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecVar instance
   */
  public static ImmutableAggSpecVar copyOf(AggSpecVar instance) {
    if (instance instanceof ImmutableAggSpecVar) {
      return (ImmutableAggSpecVar) instance;
    }
    return ImmutableAggSpecVar.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecVar ImmutableAggSpecVar}.
   * <pre>
   * ImmutableAggSpecVar.builder()
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecVar builder
   */
  public static ImmutableAggSpecVar.Builder builder() {
    return new ImmutableAggSpecVar.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecVar ImmutableAggSpecVar}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecVar", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecVar} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecVar instance) {
      Objects.requireNonNull(instance, "instance");
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecVar ImmutableAggSpecVar}.
     * @return An immutable instance of AggSpecVar
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecVar build() {
      return new ImmutableAggSpecVar(this);
    }
  }
}
