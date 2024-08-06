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
 * Immutable implementation of {@link EmMinMaxSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableEmMinMaxSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableEmMinMaxSpec.of()}.
 */
@Generated(from = "EmMinMaxSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableEmMinMaxSpec extends EmMinMaxSpec {
  private final boolean isMax;
  private final @Nullable OperationControl control;
  private final WindowScale windowScale;

  private ImmutableEmMinMaxSpec(boolean isMax, WindowScale windowScale) {
    this.isMax = isMax;
    this.windowScale = Objects.requireNonNull(windowScale, "windowScale");
    this.control = null;
  }

  private ImmutableEmMinMaxSpec(
      boolean isMax,
      @Nullable OperationControl control,
      WindowScale windowScale) {
    this.isMax = isMax;
    this.control = control;
    this.windowScale = windowScale;
  }

  /**
   * @return The value of the {@code isMax} attribute
   */
  @Override
  public boolean isMax() {
    return isMax;
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
   * Copy the current immutable object by setting a value for the {@link EmMinMaxSpec#isMax() isMax} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isMax
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableEmMinMaxSpec withIsMax(boolean value) {
    if (this.isMax == value) return this;
    return new ImmutableEmMinMaxSpec(value, this.control, this.windowScale);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link EmMinMaxSpec#control() control} attribute.
   * @param value The value for control
   * @return A modified copy of {@code this} object
   */
  public final ImmutableEmMinMaxSpec withControl(OperationControl value) {
    @Nullable OperationControl newValue = Objects.requireNonNull(value, "control");
    if (this.control == newValue) return this;
    return new ImmutableEmMinMaxSpec(this.isMax, newValue, this.windowScale);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link EmMinMaxSpec#control() control} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for control
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableEmMinMaxSpec withControl(Optional<? extends OperationControl> optional) {
    @Nullable OperationControl value = optional.orElse(null);
    if (this.control == value) return this;
    return new ImmutableEmMinMaxSpec(this.isMax, value, this.windowScale);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link EmMinMaxSpec#windowScale() windowScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for windowScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableEmMinMaxSpec withWindowScale(WindowScale value) {
    if (this.windowScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "windowScale");
    return new ImmutableEmMinMaxSpec(this.isMax, this.control, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableEmMinMaxSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableEmMinMaxSpec
        && equalTo(0, (ImmutableEmMinMaxSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableEmMinMaxSpec another) {
    return isMax == another.isMax
        && Objects.equals(control, another.control)
        && windowScale.equals(another.windowScale);
  }

  /**
   * Computes a hash code from attributes: {@code isMax}, {@code control}, {@code windowScale}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(isMax);
    h += (h << 5) + Objects.hashCode(control);
    h += (h << 5) + windowScale.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code EmMinMaxSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("EmMinMaxSpec{");
    builder.append("isMax=").append(isMax);
    if (control != null) {
      builder.append(", ");
      builder.append("control=").append(control);
    }
    builder.append(", ");
    builder.append("windowScale=").append(windowScale);
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code EmMinMaxSpec} instance.
   * @param isMax The value for the {@code isMax} attribute
   * @param windowScale The value for the {@code windowScale} attribute
   * @return An immutable EmMinMaxSpec instance
   */
  public static ImmutableEmMinMaxSpec of(boolean isMax, WindowScale windowScale) {
    return new ImmutableEmMinMaxSpec(isMax, windowScale);
  }

  /**
   * Creates an immutable copy of a {@link EmMinMaxSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable EmMinMaxSpec instance
   */
  public static ImmutableEmMinMaxSpec copyOf(EmMinMaxSpec instance) {
    if (instance instanceof ImmutableEmMinMaxSpec) {
      return (ImmutableEmMinMaxSpec) instance;
    }
    return ImmutableEmMinMaxSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableEmMinMaxSpec ImmutableEmMinMaxSpec}.
   * <pre>
   * ImmutableEmMinMaxSpec.builder()
   *    .isMax(boolean) // required {@link EmMinMaxSpec#isMax() isMax}
   *    .control(io.deephaven.api.updateby.OperationControl) // optional {@link EmMinMaxSpec#control() control}
   *    .windowScale(io.deephaven.api.updateby.spec.WindowScale) // required {@link EmMinMaxSpec#windowScale() windowScale}
   *    .build();
   * </pre>
   * @return A new ImmutableEmMinMaxSpec builder
   */
  public static ImmutableEmMinMaxSpec.Builder builder() {
    return new ImmutableEmMinMaxSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableEmMinMaxSpec ImmutableEmMinMaxSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "EmMinMaxSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_IS_MAX = 0x1L;
    private static final long INIT_BIT_WINDOW_SCALE = 0x2L;
    private long initBits = 0x3L;

    private boolean isMax;
    private @Nullable OperationControl control;
    private @Nullable WindowScale windowScale;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code EmMinMaxSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(EmMinMaxSpec instance) {
      Objects.requireNonNull(instance, "instance");
      isMax(instance.isMax());
      Optional<OperationControl> controlOptional = instance.control();
      if (controlOptional.isPresent()) {
        control(controlOptional);
      }
      windowScale(instance.windowScale());
      return this;
    }

    /**
     * Initializes the value for the {@link EmMinMaxSpec#isMax() isMax} attribute.
     * @param isMax The value for isMax 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isMax(boolean isMax) {
      this.isMax = isMax;
      initBits &= ~INIT_BIT_IS_MAX;
      return this;
    }

    /**
     * Initializes the optional value {@link EmMinMaxSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for chained invocation
     */
    public final Builder control(OperationControl control) {
      this.control = Objects.requireNonNull(control, "control");
      return this;
    }

    /**
     * Initializes the optional value {@link EmMinMaxSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder control(Optional<? extends OperationControl> control) {
      this.control = control.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link EmMinMaxSpec#windowScale() windowScale} attribute.
     * @param windowScale The value for windowScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder windowScale(WindowScale windowScale) {
      this.windowScale = Objects.requireNonNull(windowScale, "windowScale");
      initBits &= ~INIT_BIT_WINDOW_SCALE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableEmMinMaxSpec ImmutableEmMinMaxSpec}.
     * @return An immutable instance of EmMinMaxSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableEmMinMaxSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableEmMinMaxSpec(isMax, control, windowScale);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_IS_MAX) != 0) attributes.add("isMax");
      if ((initBits & INIT_BIT_WINDOW_SCALE) != 0) attributes.add("windowScale");
      return "Cannot build EmMinMaxSpec, some of required attributes are not set " + attributes;
    }
  }
}
