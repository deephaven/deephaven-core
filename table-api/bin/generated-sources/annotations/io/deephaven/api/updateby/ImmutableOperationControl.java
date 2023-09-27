package io.deephaven.api.updateby;

import java.math.MathContext;
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
 * Immutable implementation of {@link OperationControl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableOperationControl.builder()}.
 */
@Generated(from = "OperationControl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableOperationControl extends OperationControl {
  private final @Nullable BadDataBehavior onNullValue;
  private final @Nullable BadDataBehavior onNanValue;
  private final @Nullable BadDataBehavior onNullTime;
  private final @Nullable BadDataBehavior onNegativeDeltaTime;
  private final @Nullable BadDataBehavior onZeroDeltaTime;
  private final @Nullable MathContext bigValueContext;
  private transient final BadDataBehavior onNullValueOrDefault;
  private transient final BadDataBehavior onNanValueOrDefault;
  private transient final MathContext bigValueContextOrDefault;

  private ImmutableOperationControl(
      @Nullable BadDataBehavior onNullValue,
      @Nullable BadDataBehavior onNanValue,
      @Nullable BadDataBehavior onNullTime,
      @Nullable BadDataBehavior onNegativeDeltaTime,
      @Nullable BadDataBehavior onZeroDeltaTime,
      @Nullable MathContext bigValueContext) {
    this.onNullValue = onNullValue;
    this.onNanValue = onNanValue;
    this.onNullTime = onNullTime;
    this.onNegativeDeltaTime = onNegativeDeltaTime;
    this.onZeroDeltaTime = onZeroDeltaTime;
    this.bigValueContext = bigValueContext;
    this.onNullValueOrDefault = initShim.onNullValueOrDefault();
    this.onNanValueOrDefault = initShim.onNanValueOrDefault();
    this.bigValueContextOrDefault = initShim.bigValueContextOrDefault();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "OperationControl", generator = "Immutables")
  private final class InitShim {
    private byte onNullValueOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private BadDataBehavior onNullValueOrDefault;

    BadDataBehavior onNullValueOrDefault() {
      if (onNullValueOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (onNullValueOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        onNullValueOrDefaultBuildStage = STAGE_INITIALIZING;
        this.onNullValueOrDefault = Objects.requireNonNull(ImmutableOperationControl.super.onNullValueOrDefault(), "onNullValueOrDefault");
        onNullValueOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.onNullValueOrDefault;
    }

    private byte onNanValueOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private BadDataBehavior onNanValueOrDefault;

    BadDataBehavior onNanValueOrDefault() {
      if (onNanValueOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (onNanValueOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        onNanValueOrDefaultBuildStage = STAGE_INITIALIZING;
        this.onNanValueOrDefault = Objects.requireNonNull(ImmutableOperationControl.super.onNanValueOrDefault(), "onNanValueOrDefault");
        onNanValueOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.onNanValueOrDefault;
    }

    private byte bigValueContextOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private MathContext bigValueContextOrDefault;

    MathContext bigValueContextOrDefault() {
      if (bigValueContextOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (bigValueContextOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        bigValueContextOrDefaultBuildStage = STAGE_INITIALIZING;
        this.bigValueContextOrDefault = Objects.requireNonNull(ImmutableOperationControl.super.bigValueContextOrDefault(), "bigValueContextOrDefault");
        bigValueContextOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.bigValueContextOrDefault;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (onNullValueOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("onNullValueOrDefault");
      if (onNanValueOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("onNanValueOrDefault");
      if (bigValueContextOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("bigValueContextOrDefault");
      return "Cannot build OperationControl, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code onNullValue} attribute
   */
  @Override
  public Optional<BadDataBehavior> onNullValue() {
    return Optional.ofNullable(onNullValue);
  }

  /**
   * @return The value of the {@code onNanValue} attribute
   */
  @Override
  public Optional<BadDataBehavior> onNanValue() {
    return Optional.ofNullable(onNanValue);
  }

  /**
   * @return The value of the {@code onNullTime} attribute
   */
  @Override
  public Optional<BadDataBehavior> onNullTime() {
    return Optional.ofNullable(onNullTime);
  }

  /**
   * @return The value of the {@code onNegativeDeltaTime} attribute
   */
  @Override
  public Optional<BadDataBehavior> onNegativeDeltaTime() {
    return Optional.ofNullable(onNegativeDeltaTime);
  }

  /**
   * @return The value of the {@code onZeroDeltaTime} attribute
   */
  @Override
  public Optional<BadDataBehavior> onZeroDeltaTime() {
    return Optional.ofNullable(onZeroDeltaTime);
  }

  /**
   * @return The value of the {@code bigValueContext} attribute
   */
  @Override
  public Optional<MathContext> bigValueContext() {
    return Optional.ofNullable(bigValueContext);
  }

  /**
   * Get the behavior for when {@code null} values are encountered. Defaults to {@link BadDataBehavior#SKIP SKIP}.
   * 
   * @return the behavior for {@code null} values.
   */
  @Override
  public BadDataBehavior onNullValueOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.onNullValueOrDefault()
        : this.onNullValueOrDefault;
  }

  /**
   * Get the behavior for when {@link Double#NaN} values are encountered. Defaults to {@link BadDataBehavior#SKIP
   * SKIP}.
   * 
   * @return the behavior for {@link Double#NaN} values
   */
  @Override
  public BadDataBehavior onNanValueOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.onNanValueOrDefault()
        : this.onNanValueOrDefault;
  }

  /**
   * Get the {@link MathContext} to use when processing {@link java.math.BigInteger} and {@link java.math.BigDecimal}
   * values. Defaults to {@link MathContext#DECIMAL128}.
   * 
   * @return the {@link MathContext}
   */
  @Override
  public MathContext bigValueContextOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.bigValueContextOrDefault()
        : this.bigValueContextOrDefault;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#onNullValue() onNullValue} attribute.
   * @param value The value for onNullValue
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withOnNullValue(BadDataBehavior value) {
    @Nullable BadDataBehavior newValue = Objects.requireNonNull(value, "onNullValue");
    if (this.onNullValue == newValue) return this;
    return new ImmutableOperationControl(
        newValue,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#onNullValue() onNullValue} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNullValue
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withOnNullValue(Optional<? extends BadDataBehavior> optional) {
    @Nullable BadDataBehavior value = optional.orElse(null);
    if (this.onNullValue == value) return this;
    return new ImmutableOperationControl(
        value,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#onNanValue() onNanValue} attribute.
   * @param value The value for onNanValue
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withOnNanValue(BadDataBehavior value) {
    @Nullable BadDataBehavior newValue = Objects.requireNonNull(value, "onNanValue");
    if (this.onNanValue == newValue) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        newValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#onNanValue() onNanValue} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNanValue
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withOnNanValue(Optional<? extends BadDataBehavior> optional) {
    @Nullable BadDataBehavior value = optional.orElse(null);
    if (this.onNanValue == value) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        value,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#onNullTime() onNullTime} attribute.
   * @param value The value for onNullTime
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withOnNullTime(BadDataBehavior value) {
    @Nullable BadDataBehavior newValue = Objects.requireNonNull(value, "onNullTime");
    if (this.onNullTime == newValue) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        newValue,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#onNullTime() onNullTime} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNullTime
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withOnNullTime(Optional<? extends BadDataBehavior> optional) {
    @Nullable BadDataBehavior value = optional.orElse(null);
    if (this.onNullTime == value) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        value,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#onNegativeDeltaTime() onNegativeDeltaTime} attribute.
   * @param value The value for onNegativeDeltaTime
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withOnNegativeDeltaTime(BadDataBehavior value) {
    @Nullable BadDataBehavior newValue = Objects.requireNonNull(value, "onNegativeDeltaTime");
    if (this.onNegativeDeltaTime == newValue) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        newValue,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#onNegativeDeltaTime() onNegativeDeltaTime} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNegativeDeltaTime
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withOnNegativeDeltaTime(Optional<? extends BadDataBehavior> optional) {
    @Nullable BadDataBehavior value = optional.orElse(null);
    if (this.onNegativeDeltaTime == value) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        value,
        this.onZeroDeltaTime,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#onZeroDeltaTime() onZeroDeltaTime} attribute.
   * @param value The value for onZeroDeltaTime
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withOnZeroDeltaTime(BadDataBehavior value) {
    @Nullable BadDataBehavior newValue = Objects.requireNonNull(value, "onZeroDeltaTime");
    if (this.onZeroDeltaTime == newValue) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        newValue,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#onZeroDeltaTime() onZeroDeltaTime} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onZeroDeltaTime
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withOnZeroDeltaTime(Optional<? extends BadDataBehavior> optional) {
    @Nullable BadDataBehavior value = optional.orElse(null);
    if (this.onZeroDeltaTime == value) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        value,
        this.bigValueContext);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link OperationControl#bigValueContext() bigValueContext} attribute.
   * @param value The value for bigValueContext
   * @return A modified copy of {@code this} object
   */
  public final ImmutableOperationControl withBigValueContext(MathContext value) {
    @Nullable MathContext newValue = Objects.requireNonNull(value, "bigValueContext");
    if (this.bigValueContext == newValue) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link OperationControl#bigValueContext() bigValueContext} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for bigValueContext
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableOperationControl withBigValueContext(Optional<? extends MathContext> optional) {
    @Nullable MathContext value = optional.orElse(null);
    if (this.bigValueContext == value) return this;
    return new ImmutableOperationControl(
        this.onNullValue,
        this.onNanValue,
        this.onNullTime,
        this.onNegativeDeltaTime,
        this.onZeroDeltaTime,
        value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableOperationControl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableOperationControl
        && equalTo(0, (ImmutableOperationControl) another);
  }

  private boolean equalTo(int synthetic, ImmutableOperationControl another) {
    return Objects.equals(onNullValue, another.onNullValue)
        && Objects.equals(onNanValue, another.onNanValue)
        && Objects.equals(onNullTime, another.onNullTime)
        && Objects.equals(onNegativeDeltaTime, another.onNegativeDeltaTime)
        && Objects.equals(onZeroDeltaTime, another.onZeroDeltaTime)
        && Objects.equals(bigValueContext, another.bigValueContext)
        && onNullValueOrDefault.equals(another.onNullValueOrDefault)
        && onNanValueOrDefault.equals(another.onNanValueOrDefault)
        && bigValueContextOrDefault.equals(another.bigValueContextOrDefault);
  }

  /**
   * Computes a hash code from attributes: {@code onNullValue}, {@code onNanValue}, {@code onNullTime}, {@code onNegativeDeltaTime}, {@code onZeroDeltaTime}, {@code bigValueContext}, {@code onNullValueOrDefault}, {@code onNanValueOrDefault}, {@code bigValueContextOrDefault}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(onNullValue);
    h += (h << 5) + Objects.hashCode(onNanValue);
    h += (h << 5) + Objects.hashCode(onNullTime);
    h += (h << 5) + Objects.hashCode(onNegativeDeltaTime);
    h += (h << 5) + Objects.hashCode(onZeroDeltaTime);
    h += (h << 5) + Objects.hashCode(bigValueContext);
    h += (h << 5) + onNullValueOrDefault.hashCode();
    h += (h << 5) + onNanValueOrDefault.hashCode();
    h += (h << 5) + bigValueContextOrDefault.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code OperationControl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("OperationControl{");
    if (onNullValue != null) {
      builder.append("onNullValue=").append(onNullValue);
    }
    if (onNanValue != null) {
      if (builder.length() > 17) builder.append(", ");
      builder.append("onNanValue=").append(onNanValue);
    }
    if (onNullTime != null) {
      if (builder.length() > 17) builder.append(", ");
      builder.append("onNullTime=").append(onNullTime);
    }
    if (onNegativeDeltaTime != null) {
      if (builder.length() > 17) builder.append(", ");
      builder.append("onNegativeDeltaTime=").append(onNegativeDeltaTime);
    }
    if (onZeroDeltaTime != null) {
      if (builder.length() > 17) builder.append(", ");
      builder.append("onZeroDeltaTime=").append(onZeroDeltaTime);
    }
    if (bigValueContext != null) {
      if (builder.length() > 17) builder.append(", ");
      builder.append("bigValueContext=").append(bigValueContext);
    }
    if (builder.length() > 17) builder.append(", ");
    builder.append("onNullValueOrDefault=").append(onNullValueOrDefault);
    builder.append(", ");
    builder.append("onNanValueOrDefault=").append(onNanValueOrDefault);
    builder.append(", ");
    builder.append("bigValueContextOrDefault=").append(bigValueContextOrDefault);
    return builder.append("}").toString();
  }

  /**
   * Creates an immutable copy of a {@link OperationControl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable OperationControl instance
   */
  public static ImmutableOperationControl copyOf(OperationControl instance) {
    if (instance instanceof ImmutableOperationControl) {
      return (ImmutableOperationControl) instance;
    }
    return ImmutableOperationControl.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableOperationControl ImmutableOperationControl}.
   * <pre>
   * ImmutableOperationControl.builder()
   *    .onNullValue(BadDataBehavior) // optional {@link OperationControl#onNullValue() onNullValue}
   *    .onNanValue(BadDataBehavior) // optional {@link OperationControl#onNanValue() onNanValue}
   *    .onNullTime(BadDataBehavior) // optional {@link OperationControl#onNullTime() onNullTime}
   *    .onNegativeDeltaTime(BadDataBehavior) // optional {@link OperationControl#onNegativeDeltaTime() onNegativeDeltaTime}
   *    .onZeroDeltaTime(BadDataBehavior) // optional {@link OperationControl#onZeroDeltaTime() onZeroDeltaTime}
   *    .bigValueContext(java.math.MathContext) // optional {@link OperationControl#bigValueContext() bigValueContext}
   *    .build();
   * </pre>
   * @return A new ImmutableOperationControl builder
   */
  public static ImmutableOperationControl.Builder builder() {
    return new ImmutableOperationControl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableOperationControl ImmutableOperationControl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "OperationControl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements OperationControl.Builder {
    private @Nullable BadDataBehavior onNullValue;
    private @Nullable BadDataBehavior onNanValue;
    private @Nullable BadDataBehavior onNullTime;
    private @Nullable BadDataBehavior onNegativeDeltaTime;
    private @Nullable BadDataBehavior onZeroDeltaTime;
    private @Nullable MathContext bigValueContext;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code OperationControl} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(OperationControl instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<BadDataBehavior> onNullValueOptional = instance.onNullValue();
      if (onNullValueOptional.isPresent()) {
        onNullValue(onNullValueOptional);
      }
      Optional<BadDataBehavior> onNanValueOptional = instance.onNanValue();
      if (onNanValueOptional.isPresent()) {
        onNanValue(onNanValueOptional);
      }
      Optional<BadDataBehavior> onNullTimeOptional = instance.onNullTime();
      if (onNullTimeOptional.isPresent()) {
        onNullTime(onNullTimeOptional);
      }
      Optional<BadDataBehavior> onNegativeDeltaTimeOptional = instance.onNegativeDeltaTime();
      if (onNegativeDeltaTimeOptional.isPresent()) {
        onNegativeDeltaTime(onNegativeDeltaTimeOptional);
      }
      Optional<BadDataBehavior> onZeroDeltaTimeOptional = instance.onZeroDeltaTime();
      if (onZeroDeltaTimeOptional.isPresent()) {
        onZeroDeltaTime(onZeroDeltaTimeOptional);
      }
      Optional<MathContext> bigValueContextOptional = instance.bigValueContext();
      if (bigValueContextOptional.isPresent()) {
        bigValueContext(bigValueContextOptional);
      }
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNullValue() onNullValue} to onNullValue.
     * @param onNullValue The value for onNullValue
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNullValue(BadDataBehavior onNullValue) {
      this.onNullValue = Objects.requireNonNull(onNullValue, "onNullValue");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNullValue() onNullValue} to onNullValue.
     * @param onNullValue The value for onNullValue
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNullValue(Optional<? extends BadDataBehavior> onNullValue) {
      this.onNullValue = onNullValue.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNanValue() onNanValue} to onNanValue.
     * @param onNanValue The value for onNanValue
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNanValue(BadDataBehavior onNanValue) {
      this.onNanValue = Objects.requireNonNull(onNanValue, "onNanValue");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNanValue() onNanValue} to onNanValue.
     * @param onNanValue The value for onNanValue
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNanValue(Optional<? extends BadDataBehavior> onNanValue) {
      this.onNanValue = onNanValue.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNullTime() onNullTime} to onNullTime.
     * @param onNullTime The value for onNullTime
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNullTime(BadDataBehavior onNullTime) {
      this.onNullTime = Objects.requireNonNull(onNullTime, "onNullTime");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNullTime() onNullTime} to onNullTime.
     * @param onNullTime The value for onNullTime
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNullTime(Optional<? extends BadDataBehavior> onNullTime) {
      this.onNullTime = onNullTime.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNegativeDeltaTime() onNegativeDeltaTime} to onNegativeDeltaTime.
     * @param onNegativeDeltaTime The value for onNegativeDeltaTime
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNegativeDeltaTime(BadDataBehavior onNegativeDeltaTime) {
      this.onNegativeDeltaTime = Objects.requireNonNull(onNegativeDeltaTime, "onNegativeDeltaTime");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onNegativeDeltaTime() onNegativeDeltaTime} to onNegativeDeltaTime.
     * @param onNegativeDeltaTime The value for onNegativeDeltaTime
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNegativeDeltaTime(Optional<? extends BadDataBehavior> onNegativeDeltaTime) {
      this.onNegativeDeltaTime = onNegativeDeltaTime.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onZeroDeltaTime() onZeroDeltaTime} to onZeroDeltaTime.
     * @param onZeroDeltaTime The value for onZeroDeltaTime
     * @return {@code this} builder for chained invocation
     */
    public final Builder onZeroDeltaTime(BadDataBehavior onZeroDeltaTime) {
      this.onZeroDeltaTime = Objects.requireNonNull(onZeroDeltaTime, "onZeroDeltaTime");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#onZeroDeltaTime() onZeroDeltaTime} to onZeroDeltaTime.
     * @param onZeroDeltaTime The value for onZeroDeltaTime
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onZeroDeltaTime(Optional<? extends BadDataBehavior> onZeroDeltaTime) {
      this.onZeroDeltaTime = onZeroDeltaTime.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#bigValueContext() bigValueContext} to bigValueContext.
     * @param bigValueContext The value for bigValueContext
     * @return {@code this} builder for chained invocation
     */
    public final Builder bigValueContext(MathContext bigValueContext) {
      this.bigValueContext = Objects.requireNonNull(bigValueContext, "bigValueContext");
      return this;
    }

    /**
     * Initializes the optional value {@link OperationControl#bigValueContext() bigValueContext} to bigValueContext.
     * @param bigValueContext The value for bigValueContext
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder bigValueContext(Optional<? extends MathContext> bigValueContext) {
      this.bigValueContext = bigValueContext.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableOperationControl ImmutableOperationControl}.
     * @return An immutable instance of OperationControl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableOperationControl build() {
      return new ImmutableOperationControl(onNullValue, onNanValue, onNullTime, onNegativeDeltaTime, onZeroDeltaTime, bigValueContext);
    }
  }
}
