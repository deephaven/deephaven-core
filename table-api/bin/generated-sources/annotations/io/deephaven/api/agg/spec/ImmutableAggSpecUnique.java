package io.deephaven.api.agg.spec;

import io.deephaven.api.object.UnionObject;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link AggSpecUnique}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableAggSpecUnique.builder()}.
 */
@Generated(from = "AggSpecUnique", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableAggSpecUnique extends AggSpecUnique {
  private final boolean includeNulls;
  private final @Nullable UnionObject nonUniqueSentinel;

  private ImmutableAggSpecUnique(ImmutableAggSpecUnique.Builder builder) {
    this.nonUniqueSentinel = builder.nonUniqueSentinel;
    this.includeNulls = builder.includeNullsIsSet()
        ? builder.includeNulls
        : super.includeNulls();
  }

  private ImmutableAggSpecUnique(
      boolean includeNulls,
      @Nullable UnionObject nonUniqueSentinel) {
    this.includeNulls = includeNulls;
    this.nonUniqueSentinel = nonUniqueSentinel;
  }

  /**
   * Whether to include {@code null} values as a distinct value for determining if there is only one unique value to
   * output.
   * @return Whether to include nulls
   */
  @Override
  public boolean includeNulls() {
    return includeNulls;
  }

  /**
   * The output value to use for groups that don't have a single unique input value.
   * @return The non-unique sentinel value
   */
  @Override
  public Optional<UnionObject> nonUniqueSentinel() {
    return Optional.ofNullable(nonUniqueSentinel);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AggSpecUnique#includeNulls() includeNulls} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for includeNulls
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableAggSpecUnique withIncludeNulls(boolean value) {
    if (this.includeNulls == value) return this;
    return new ImmutableAggSpecUnique(value, this.nonUniqueSentinel);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link AggSpecUnique#nonUniqueSentinel() nonUniqueSentinel} attribute.
   * @param value The value for nonUniqueSentinel
   * @return A modified copy of {@code this} object
   */
  public final ImmutableAggSpecUnique withNonUniqueSentinel(UnionObject value) {
    @Nullable UnionObject newValue = Objects.requireNonNull(value, "nonUniqueSentinel");
    if (this.nonUniqueSentinel == newValue) return this;
    return new ImmutableAggSpecUnique(this.includeNulls, newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link AggSpecUnique#nonUniqueSentinel() nonUniqueSentinel} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for nonUniqueSentinel
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableAggSpecUnique withNonUniqueSentinel(Optional<? extends UnionObject> optional) {
    @Nullable UnionObject value = optional.orElse(null);
    if (this.nonUniqueSentinel == value) return this;
    return new ImmutableAggSpecUnique(this.includeNulls, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableAggSpecUnique} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableAggSpecUnique
        && equalTo(0, (ImmutableAggSpecUnique) another);
  }

  private boolean equalTo(int synthetic, ImmutableAggSpecUnique another) {
    return includeNulls == another.includeNulls
        && Objects.equals(nonUniqueSentinel, another.nonUniqueSentinel);
  }

  /**
   * Computes a hash code from attributes: {@code includeNulls}, {@code nonUniqueSentinel}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(includeNulls);
    h += (h << 5) + Objects.hashCode(nonUniqueSentinel);
    return h;
  }

  /**
   * Prints the immutable value {@code AggSpecUnique} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AggSpecUnique{");
    builder.append("includeNulls=").append(includeNulls);
    if (nonUniqueSentinel != null) {
      builder.append(", ");
      builder.append("nonUniqueSentinel=").append(nonUniqueSentinel);
    }
    return builder.append("}").toString();
  }

  /**
   * Creates an immutable copy of a {@link AggSpecUnique} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable AggSpecUnique instance
   */
  public static ImmutableAggSpecUnique copyOf(AggSpecUnique instance) {
    if (instance instanceof ImmutableAggSpecUnique) {
      return (ImmutableAggSpecUnique) instance;
    }
    return ImmutableAggSpecUnique.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableAggSpecUnique ImmutableAggSpecUnique}.
   * <pre>
   * ImmutableAggSpecUnique.builder()
   *    .includeNulls(boolean) // optional {@link AggSpecUnique#includeNulls() includeNulls}
   *    .nonUniqueSentinel(io.deephaven.api.object.UnionObject) // optional {@link AggSpecUnique#nonUniqueSentinel() nonUniqueSentinel}
   *    .build();
   * </pre>
   * @return A new ImmutableAggSpecUnique builder
   */
  public static ImmutableAggSpecUnique.Builder builder() {
    return new ImmutableAggSpecUnique.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableAggSpecUnique ImmutableAggSpecUnique}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AggSpecUnique", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long OPT_BIT_INCLUDE_NULLS = 0x1L;
    private long optBits;

    private boolean includeNulls;
    private @Nullable UnionObject nonUniqueSentinel;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AggSpecUnique} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(AggSpecUnique instance) {
      Objects.requireNonNull(instance, "instance");
      includeNulls(instance.includeNulls());
      Optional<UnionObject> nonUniqueSentinelOptional = instance.nonUniqueSentinel();
      if (nonUniqueSentinelOptional.isPresent()) {
        nonUniqueSentinel(nonUniqueSentinelOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link AggSpecUnique#includeNulls() includeNulls} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link AggSpecUnique#includeNulls() includeNulls}.</em>
     * @param includeNulls The value for includeNulls 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder includeNulls(boolean includeNulls) {
      this.includeNulls = includeNulls;
      optBits |= OPT_BIT_INCLUDE_NULLS;
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecUnique#nonUniqueSentinel() nonUniqueSentinel} to nonUniqueSentinel.
     * @param nonUniqueSentinel The value for nonUniqueSentinel
     * @return {@code this} builder for chained invocation
     */
    public final Builder nonUniqueSentinel(UnionObject nonUniqueSentinel) {
      this.nonUniqueSentinel = Objects.requireNonNull(nonUniqueSentinel, "nonUniqueSentinel");
      return this;
    }

    /**
     * Initializes the optional value {@link AggSpecUnique#nonUniqueSentinel() nonUniqueSentinel} to nonUniqueSentinel.
     * @param nonUniqueSentinel The value for nonUniqueSentinel
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder nonUniqueSentinel(Optional<? extends UnionObject> nonUniqueSentinel) {
      this.nonUniqueSentinel = nonUniqueSentinel.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableAggSpecUnique ImmutableAggSpecUnique}.
     * @return An immutable instance of AggSpecUnique
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableAggSpecUnique build() {
      return new ImmutableAggSpecUnique(this);
    }

    private boolean includeNullsIsSet() {
      return (optBits & OPT_BIT_INCLUDE_NULLS) != 0;
    }
  }
}
