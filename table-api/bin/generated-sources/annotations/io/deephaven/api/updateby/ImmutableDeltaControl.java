package io.deephaven.api.updateby;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DeltaControl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDeltaControl.builder()}.
 */
@Generated(from = "DeltaControl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableDeltaControl extends DeltaControl {
  private final NullBehavior nullBehavior;

  private ImmutableDeltaControl(ImmutableDeltaControl.Builder builder) {
    this.nullBehavior = builder.nullBehavior != null
        ? builder.nullBehavior
        : Objects.requireNonNull(super.nullBehavior(), "nullBehavior");
  }

  private ImmutableDeltaControl(NullBehavior nullBehavior) {
    this.nullBehavior = nullBehavior;
  }

  /**
   * Get the behavior of the Delta operation when null values are encountered.
   * 
   * @return the {@link NullBehavior}
   */
  @Override
  public NullBehavior nullBehavior() {
    return nullBehavior;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeltaControl#nullBehavior() nullBehavior} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for nullBehavior
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeltaControl withNullBehavior(NullBehavior value) {
    NullBehavior newValue = Objects.requireNonNull(value, "nullBehavior");
    if (this.nullBehavior == newValue) return this;
    return new ImmutableDeltaControl(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDeltaControl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDeltaControl
        && equalTo(0, (ImmutableDeltaControl) another);
  }

  private boolean equalTo(int synthetic, ImmutableDeltaControl another) {
    return nullBehavior.equals(another.nullBehavior);
  }

  /**
   * Computes a hash code from attributes: {@code nullBehavior}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + nullBehavior.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DeltaControl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "DeltaControl{"
        + "nullBehavior=" + nullBehavior
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link DeltaControl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DeltaControl instance
   */
  public static ImmutableDeltaControl copyOf(DeltaControl instance) {
    if (instance instanceof ImmutableDeltaControl) {
      return (ImmutableDeltaControl) instance;
    }
    return ImmutableDeltaControl.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDeltaControl ImmutableDeltaControl}.
   * <pre>
   * ImmutableDeltaControl.builder()
   *    .nullBehavior(io.deephaven.api.updateby.NullBehavior) // optional {@link DeltaControl#nullBehavior() nullBehavior}
   *    .build();
   * </pre>
   * @return A new ImmutableDeltaControl builder
   */
  public static ImmutableDeltaControl.Builder builder() {
    return new ImmutableDeltaControl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDeltaControl ImmutableDeltaControl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DeltaControl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements DeltaControl.Builder {
    private @Nullable NullBehavior nullBehavior;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DeltaControl} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(DeltaControl instance) {
      Objects.requireNonNull(instance, "instance");
      nullBehavior(instance.nullBehavior());
      return this;
    }

    /**
     * Initializes the value for the {@link DeltaControl#nullBehavior() nullBehavior} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DeltaControl#nullBehavior() nullBehavior}.</em>
     * @param nullBehavior The value for nullBehavior 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder nullBehavior(NullBehavior nullBehavior) {
      this.nullBehavior = Objects.requireNonNull(nullBehavior, "nullBehavior");
      return this;
    }

    /**
     * Builds a new {@link ImmutableDeltaControl ImmutableDeltaControl}.
     * @return An immutable instance of DeltaControl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDeltaControl build() {
      return new ImmutableDeltaControl(this);
    }
  }
}
