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
 * Immutable implementation of {@link EmsSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableEmsSpec.builder()}.
 */
@Generated(from = "EmsSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableEmsSpec extends EmsSpec {
  private final @Nullable OperationControl control;
  private final WindowScale timeScale;

  private ImmutableEmsSpec(
      @Nullable OperationControl control,
      WindowScale timeScale) {
    this.control = control;
    this.timeScale = timeScale;
  }

  /**
   * @return The value of the {@code control} attribute
   */
  @Override
  public Optional<OperationControl> control() {
    return Optional.ofNullable(control);
  }

  /**
   * @return The value of the {@code timeScale} attribute
   */
  @Override
  public WindowScale timeScale() {
    return timeScale;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link EmsSpec#control() control} attribute.
   * @param value The value for control
   * @return A modified copy of {@code this} object
   */
  public final ImmutableEmsSpec withControl(OperationControl value) {
    @Nullable OperationControl newValue = Objects.requireNonNull(value, "control");
    if (this.control == newValue) return this;
    return new ImmutableEmsSpec(newValue, this.timeScale);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link EmsSpec#control() control} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for control
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableEmsSpec withControl(Optional<? extends OperationControl> optional) {
    @Nullable OperationControl value = optional.orElse(null);
    if (this.control == value) return this;
    return new ImmutableEmsSpec(value, this.timeScale);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link EmsSpec#timeScale() timeScale} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timeScale
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableEmsSpec withTimeScale(WindowScale value) {
    if (this.timeScale == value) return this;
    WindowScale newValue = Objects.requireNonNull(value, "timeScale");
    return new ImmutableEmsSpec(this.control, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableEmsSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableEmsSpec
        && equalTo(0, (ImmutableEmsSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableEmsSpec another) {
    return Objects.equals(control, another.control)
        && timeScale.equals(another.timeScale);
  }

  /**
   * Computes a hash code from attributes: {@code control}, {@code timeScale}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(control);
    h += (h << 5) + timeScale.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code EmsSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("EmsSpec{");
    if (control != null) {
      builder.append("control=").append(control);
    }
    if (builder.length() > 8) builder.append(", ");
    builder.append("timeScale=").append(timeScale);
    return builder.append("}").toString();
  }

  /**
   * Creates an immutable copy of a {@link EmsSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable EmsSpec instance
   */
  public static ImmutableEmsSpec copyOf(EmsSpec instance) {
    if (instance instanceof ImmutableEmsSpec) {
      return (ImmutableEmsSpec) instance;
    }
    return ImmutableEmsSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableEmsSpec ImmutableEmsSpec}.
   * <pre>
   * ImmutableEmsSpec.builder()
   *    .control(io.deephaven.api.updateby.OperationControl) // optional {@link EmsSpec#control() control}
   *    .timeScale(io.deephaven.api.updateby.spec.WindowScale) // required {@link EmsSpec#timeScale() timeScale}
   *    .build();
   * </pre>
   * @return A new ImmutableEmsSpec builder
   */
  public static ImmutableEmsSpec.Builder builder() {
    return new ImmutableEmsSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableEmsSpec ImmutableEmsSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "EmsSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TIME_SCALE = 0x1L;
    private long initBits = 0x1L;

    private @Nullable OperationControl control;
    private @Nullable WindowScale timeScale;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code EmsSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(EmsSpec instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<OperationControl> controlOptional = instance.control();
      if (controlOptional.isPresent()) {
        control(controlOptional);
      }
      timeScale(instance.timeScale());
      return this;
    }

    /**
     * Initializes the optional value {@link EmsSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for chained invocation
     */
    public final Builder control(OperationControl control) {
      this.control = Objects.requireNonNull(control, "control");
      return this;
    }

    /**
     * Initializes the optional value {@link EmsSpec#control() control} to control.
     * @param control The value for control
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder control(Optional<? extends OperationControl> control) {
      this.control = control.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link EmsSpec#timeScale() timeScale} attribute.
     * @param timeScale The value for timeScale 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder timeScale(WindowScale timeScale) {
      this.timeScale = Objects.requireNonNull(timeScale, "timeScale");
      initBits &= ~INIT_BIT_TIME_SCALE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableEmsSpec ImmutableEmsSpec}.
     * @return An immutable instance of EmsSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableEmsSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableEmsSpec(control, timeScale);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TIME_SCALE) != 0) attributes.add("timeScale");
      return "Cannot build EmsSpec, some of required attributes are not set " + attributes;
    }
  }
}
