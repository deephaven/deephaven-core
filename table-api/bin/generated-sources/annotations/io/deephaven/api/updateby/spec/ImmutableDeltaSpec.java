package io.deephaven.api.updateby.spec;

import io.deephaven.api.updateby.DeltaControl;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DeltaSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDeltaSpec.builder()}.
 */
@Generated(from = "DeltaSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableDeltaSpec extends DeltaSpec {
  private final DeltaControl deltaControl;

  private ImmutableDeltaSpec(ImmutableDeltaSpec.Builder builder) {
    this.deltaControl = builder.deltaControl != null
        ? builder.deltaControl
        : Objects.requireNonNull(super.deltaControl(), "deltaControl");
  }

  private ImmutableDeltaSpec(DeltaControl deltaControl) {
    this.deltaControl = deltaControl;
  }

  /**
   * @return The value of the {@code deltaControl} attribute
   */
  @Override
  public DeltaControl deltaControl() {
    return deltaControl;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeltaSpec#deltaControl() deltaControl} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for deltaControl
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeltaSpec withDeltaControl(DeltaControl value) {
    if (this.deltaControl == value) return this;
    DeltaControl newValue = Objects.requireNonNull(value, "deltaControl");
    return new ImmutableDeltaSpec(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDeltaSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDeltaSpec
        && equalTo(0, (ImmutableDeltaSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableDeltaSpec another) {
    return deltaControl.equals(another.deltaControl);
  }

  /**
   * Computes a hash code from attributes: {@code deltaControl}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + deltaControl.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DeltaSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "DeltaSpec{"
        + "deltaControl=" + deltaControl
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link DeltaSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DeltaSpec instance
   */
  public static ImmutableDeltaSpec copyOf(DeltaSpec instance) {
    if (instance instanceof ImmutableDeltaSpec) {
      return (ImmutableDeltaSpec) instance;
    }
    return ImmutableDeltaSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDeltaSpec ImmutableDeltaSpec}.
   * <pre>
   * ImmutableDeltaSpec.builder()
   *    .deltaControl(io.deephaven.api.updateby.DeltaControl) // optional {@link DeltaSpec#deltaControl() deltaControl}
   *    .build();
   * </pre>
   * @return A new ImmutableDeltaSpec builder
   */
  public static ImmutableDeltaSpec.Builder builder() {
    return new ImmutableDeltaSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDeltaSpec ImmutableDeltaSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DeltaSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private @Nullable DeltaControl deltaControl;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DeltaSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(DeltaSpec instance) {
      Objects.requireNonNull(instance, "instance");
      deltaControl(instance.deltaControl());
      return this;
    }

    /**
     * Initializes the value for the {@link DeltaSpec#deltaControl() deltaControl} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DeltaSpec#deltaControl() deltaControl}.</em>
     * @param deltaControl The value for deltaControl 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder deltaControl(DeltaControl deltaControl) {
      this.deltaControl = Objects.requireNonNull(deltaControl, "deltaControl");
      return this;
    }

    /**
     * Builds a new {@link ImmutableDeltaSpec ImmutableDeltaSpec}.
     * @return An immutable instance of DeltaSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDeltaSpec build() {
      return new ImmutableDeltaSpec(this);
    }
  }
}
