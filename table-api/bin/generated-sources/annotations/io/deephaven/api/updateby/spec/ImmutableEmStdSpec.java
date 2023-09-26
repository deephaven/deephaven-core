package io.deephaven.api.updateby.spec;

import io.deephaven.api.updateby.OperationControl;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link EmStdSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableEmStdSpec.builder()}.
 */
@Generated(from = "EmStdSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableEmStdSpec extends EmStdSpec {
  private final @Nullable OperationControl control;
  private final WindowScale windowScale;

  private ImmutableEmStdSpec(
      @Nullable OperationControl control,
      WindowScale windowScale) {
    this.control = control;
    this.windowScale = windowScale;
  }

  /**
   * @return The value of the {@code control} attribute
   */
  @Override
  public Optional<OperationControl> control() {
    return Optional.ofNullable(control);
  }

  /**
   * @return The value of the {@code windowScale} attribute
   */
  @Override
  public WindowScale windowScale() {
    return windowScale;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link EmStdSpec#control() control} attribute.
   * @param value The value for control
   * @return A modified copy of {@code this} object
   */
  public final ImmutableEmStdSpec withControl(OperationControl value) {
    @Nullable OperationControl newValue = Objects.requireNonNull(value, "control");
    if (this.control == newValue) return this;
    return new ImmutableEmStdSpec(newValue, this.windowScale);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link EmStdSpec#control() control} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for control
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableEmStdSpec withControl(Optional<? extends OperationControl> optional) {
    @Nullable OperationControl value = optional.orElse(null);
    if (this.control == value) return this;
    return new ImmutableEmStdSpec(value, this.windowScale);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link EmStdSpec#windowScale() windowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for windowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableEmStdSpec withWindowScale(WindowScale value) {
    if (this.windowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "windowScale");
    return new ImmutableEmStdSpec(this.control, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableEmStdSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableEmStdSpec
        && equalTo(0, (ImmutableEmStdSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableEmStdSpec another) {
    return Objects.equals(control, another.control)
        && windowScale.equals(another.windowScale);
  }

  /**
   * Computes a hash code from attributes: {@code control}, {@code windowScale}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(control);
    h += (h << 5) + windowScale.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code EmStdSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("EmStdSpec{");
    if (control != null) {
      builder.append("control=").append(control);
    }
    if (builder.length() > 10) builder.append(", ");
    builder.append("windowScale=").append(windowScale);
    return builder.append("}").toString();
  }

  /**
   * Creates an immutable copy of a {@link EmStdSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable EmStdSpec instance
   */
  public static ImmutableEmStdSpec copyOf(EmStdSpec instance) {
    if (instance instanceof ImmutableEmStdSpec) {
      return (ImmutableEmStdSpec) instance;
    }
    return ImmutableEmStdSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableEmStdSpec ImmutableEmStdSpec}.
   * <pre>
   * ImmutableEmStdSpec.builder()
   *    .control(io.deephaven.api.updateby.OperationControl) // optional {@link EmStdSpec#control() control}
   *    .windowScale(io.deephaven.api.updateby.spec.WindowScale) // required {@link EmStdSpec#windowScale() windowScale}
   *    .build();
   * </pre>
   * @return A new ImmutableEmStdSpec builder
   */
  public static ImmutableEmStdSpec.Builder builder() {
    return new ImmutableEmStdSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableEmStdSpec ImmutableEmStdSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "EmStdSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_WINDOW_SCALE = 0x1L;
    private long initBits = 0x1L;

    private @Nullable OperationControl control;
    private @Nullable WindowScale windowScale;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code EmStdSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(EmStdSpec instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<OperationControl> controlOptional = instance.control();
      if (controlOptional.isPresent()) {
        control(controlOptional);
      }
      windowScale(instance.windowScale());
      return this;
    }

    /**
     * Initializes the optional value {@link EmStdSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for chained invocation
     */
    public final Builder control(OperationControl control) {
      this.control = Objects.requireNonNull(control, "control");
      return this;
    }

    /**
     * Initializes the optional value {@link EmStdSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder control(Optional<? extends OperationControl> control) {
      this.control = control.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link EmStdSpec#windowScale() windowScale} attribute.
     * @param windowScale The value for windowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder windowScale(WindowScale windowScale) {
      this.windowScale = Objects.requireNonNull(windowScale, "windowScale");
      initBits &= ~INIT_BIT_WINDOW_SCALE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableEmStdSpec ImmutableEmStdSpec}.
     * @return An immutable instance of EmStdSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableEmStdSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableEmStdSpec(control, windowScale);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_WINDOW_SCALE) != 0) attributes.add("windowScale");
      return "Cannot build EmStdSpec, some of required attributes are not set " + attributes;
    }
  }
}
