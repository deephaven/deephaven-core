package io.deephaven.api.updateby;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link UpdateByControl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableUpdateByControl.builder()}.
 */
@Generated(from = "UpdateByControl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableUpdateByControl extends UpdateByControl {
  private final @Nullable Boolean useRedirection;
  private final @Nullable Integer chunkCapacity;
  private final @Nullable Double maxStaticSparseMemoryOverhead;
  private final @Nullable Integer initialHashTableSize;
  private final @Nullable Double maximumLoadFactor;
  private final @Nullable Double targetLoadFactor;
  private final @Nullable MathContext mathContext;
  private transient final boolean useRedirectionOrDefault;
  private transient final int chunkCapacityOrDefault;
  private transient final double maxStaticSparseMemoryOverheadOrDefault;
  private transient final int initialHashTableSizeOrDefault;
  private transient final double maximumLoadFactorOrDefault;
  private transient final double targetLoadFactorOrDefault;
  private transient final MathContext mathContextOrDefault;

  private ImmutableUpdateByControl(
      @Nullable Boolean useRedirection,
      @Nullable Integer chunkCapacity,
      @Nullable Double maxStaticSparseMemoryOverhead,
      @Nullable Integer initialHashTableSize,
      @Nullable Double maximumLoadFactor,
      @Nullable Double targetLoadFactor,
      @Nullable MathContext mathContext) {
    this.useRedirection = useRedirection;
    this.chunkCapacity = chunkCapacity;
    this.maxStaticSparseMemoryOverhead = maxStaticSparseMemoryOverhead;
    this.initialHashTableSize = initialHashTableSize;
    this.maximumLoadFactor = maximumLoadFactor;
    this.targetLoadFactor = targetLoadFactor;
    this.mathContext = mathContext;
    this.useRedirectionOrDefault = initShim.useRedirectionOrDefault();
    this.chunkCapacityOrDefault = initShim.chunkCapacityOrDefault();
    this.maxStaticSparseMemoryOverheadOrDefault = initShim.maxStaticSparseMemoryOverheadOrDefault();
    this.initialHashTableSizeOrDefault = initShim.initialHashTableSizeOrDefault();
    this.maximumLoadFactorOrDefault = initShim.maximumLoadFactorOrDefault();
    this.targetLoadFactorOrDefault = initShim.targetLoadFactorOrDefault();
    this.mathContextOrDefault = initShim.mathContextOrDefault();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "UpdateByControl", generator = "Immutables")
  private final class InitShim {
    private byte useRedirectionOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private boolean useRedirectionOrDefault;

    boolean useRedirectionOrDefault() {
      if (useRedirectionOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (useRedirectionOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        useRedirectionOrDefaultBuildStage = STAGE_INITIALIZING;
        this.useRedirectionOrDefault = ImmutableUpdateByControl.super.useRedirectionOrDefault();
        useRedirectionOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.useRedirectionOrDefault;
    }

    private byte chunkCapacityOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private int chunkCapacityOrDefault;

    int chunkCapacityOrDefault() {
      if (chunkCapacityOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (chunkCapacityOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        chunkCapacityOrDefaultBuildStage = STAGE_INITIALIZING;
        this.chunkCapacityOrDefault = ImmutableUpdateByControl.super.chunkCapacityOrDefault();
        chunkCapacityOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.chunkCapacityOrDefault;
    }

    private byte maxStaticSparseMemoryOverheadOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private double maxStaticSparseMemoryOverheadOrDefault;

    double maxStaticSparseMemoryOverheadOrDefault() {
      if (maxStaticSparseMemoryOverheadOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maxStaticSparseMemoryOverheadOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        maxStaticSparseMemoryOverheadOrDefaultBuildStage = STAGE_INITIALIZING;
        this.maxStaticSparseMemoryOverheadOrDefault = ImmutableUpdateByControl.super.maxStaticSparseMemoryOverheadOrDefault();
        maxStaticSparseMemoryOverheadOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.maxStaticSparseMemoryOverheadOrDefault;
    }

    private byte initialHashTableSizeOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private int initialHashTableSizeOrDefault;

    int initialHashTableSizeOrDefault() {
      if (initialHashTableSizeOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (initialHashTableSizeOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        initialHashTableSizeOrDefaultBuildStage = STAGE_INITIALIZING;
        this.initialHashTableSizeOrDefault = ImmutableUpdateByControl.super.initialHashTableSizeOrDefault();
        initialHashTableSizeOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.initialHashTableSizeOrDefault;
    }

    private byte maximumLoadFactorOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private double maximumLoadFactorOrDefault;

    double maximumLoadFactorOrDefault() {
      if (maximumLoadFactorOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maximumLoadFactorOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        maximumLoadFactorOrDefaultBuildStage = STAGE_INITIALIZING;
        this.maximumLoadFactorOrDefault = ImmutableUpdateByControl.super.maximumLoadFactorOrDefault();
        maximumLoadFactorOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.maximumLoadFactorOrDefault;
    }

    private byte targetLoadFactorOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private double targetLoadFactorOrDefault;

    double targetLoadFactorOrDefault() {
      if (targetLoadFactorOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (targetLoadFactorOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        targetLoadFactorOrDefaultBuildStage = STAGE_INITIALIZING;
        this.targetLoadFactorOrDefault = ImmutableUpdateByControl.super.targetLoadFactorOrDefault();
        targetLoadFactorOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.targetLoadFactorOrDefault;
    }

    private byte mathContextOrDefaultBuildStage = STAGE_UNINITIALIZED;
    private MathContext mathContextOrDefault;

    MathContext mathContextOrDefault() {
      if (mathContextOrDefaultBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (mathContextOrDefaultBuildStage == STAGE_UNINITIALIZED) {
        mathContextOrDefaultBuildStage = STAGE_INITIALIZING;
        this.mathContextOrDefault = Objects.requireNonNull(ImmutableUpdateByControl.super.mathContextOrDefault(), "mathContextOrDefault");
        mathContextOrDefaultBuildStage = STAGE_INITIALIZED;
      }
      return this.mathContextOrDefault;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (useRedirectionOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("useRedirectionOrDefault");
      if (chunkCapacityOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("chunkCapacityOrDefault");
      if (maxStaticSparseMemoryOverheadOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("maxStaticSparseMemoryOverheadOrDefault");
      if (initialHashTableSizeOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("initialHashTableSizeOrDefault");
      if (maximumLoadFactorOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("maximumLoadFactorOrDefault");
      if (targetLoadFactorOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("targetLoadFactorOrDefault");
      if (mathContextOrDefaultBuildStage == STAGE_INITIALIZING) attributes.add("mathContextOrDefault");
      return "Cannot build UpdateByControl, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * If redirections should be used for output sources instead of sparse array sources.
   */
  @Override
  public @Nullable Boolean useRedirection() {
    return useRedirection;
  }

  /**
   * The maximum chunk capacity.
   */
  @Override
  public OptionalInt chunkCapacity() {
    return chunkCapacity != null
        ? OptionalInt.of(chunkCapacity)
        : OptionalInt.empty();
  }

  /**
   * The maximum fractional memory overhead allowable for sparse redirections as a fraction (e.g. 1.1 is 10%
   * overhead). Values less than zero disable overhead checking, and result in always using the sparse structure. A
   * value of zero results in never using the sparse structure.
   */
  @Override
  public OptionalDouble maxStaticSparseMemoryOverhead() {
    return maxStaticSparseMemoryOverhead != null
        ? OptionalDouble.of(maxStaticSparseMemoryOverhead)
        : OptionalDouble.empty();
  }

  /**
   * The initial hash table size.
   */
  @Override
  public OptionalInt initialHashTableSize() {
    return initialHashTableSize != null
        ? OptionalInt.of(initialHashTableSize)
        : OptionalInt.empty();
  }

  /**
   * The maximum load factor for the hash table.
   */
  @Override
  public OptionalDouble maximumLoadFactor() {
    return maximumLoadFactor != null
        ? OptionalDouble.of(maximumLoadFactor)
        : OptionalDouble.empty();
  }

  /**
   * The target load factor for the hash table.
   */
  @Override
  public OptionalDouble targetLoadFactor() {
    return targetLoadFactor != null
        ? OptionalDouble.of(targetLoadFactor)
        : OptionalDouble.empty();
  }

  /**
   * The math context.
   */
  @Override
  public Optional<MathContext> mathContext() {
    return Optional.ofNullable(mathContext);
  }

  /**
   * Equivalent to {@code useRedirection() == null ? useRedirectionDefault() : useRedirection()}.
   * @see #useRedirectionDefault()
   */
  @Override
  public boolean useRedirectionOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.useRedirectionOrDefault()
        : this.useRedirectionOrDefault;
  }

  /**
   * Equivalent to {@code chunkCapacity().orElseGet(UpdateByControl::chunkCapacityDefault)}.
   * @see #chunkCapacityDefault()
   */
  @Override
  public int chunkCapacityOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.chunkCapacityOrDefault()
        : this.chunkCapacityOrDefault;
  }

  /**
   * Equivalent to
   * {@code maxStaticSparseMemoryOverhead().orElseGet(UpdateByControl::maximumStaticMemoryOverheadDefault)}.
   * @see #maximumStaticMemoryOverheadDefault()
   */
  @Override
  public double maxStaticSparseMemoryOverheadOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.maxStaticSparseMemoryOverheadOrDefault()
        : this.maxStaticSparseMemoryOverheadOrDefault;
  }

  /**
   * Equivalent to {@code initialHashTableSize().orElseGet(UpdateByControl::initialHashTableSizeDefault)}.
   * @see #initialHashTableSizeDefault()
   */
  @Override
  public int initialHashTableSizeOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.initialHashTableSizeOrDefault()
        : this.initialHashTableSizeOrDefault;
  }

  /**
   * Equivalent to {@code maximumLoadFactor().orElseGet(UpdateByControl::maximumLoadFactorDefault)}.
   * @see #maximumLoadFactorDefault()
   */
  @Override
  public double maximumLoadFactorOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.maximumLoadFactorOrDefault()
        : this.maximumLoadFactorOrDefault;
  }

  /**
   * Equivalent to {@code targetLoadFactor().orElseGet(UpdateByControl::targetLoadFactorDefault)}.
   * @see #targetLoadFactorDefault()
   */
  @Override
  public double targetLoadFactorOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.targetLoadFactorOrDefault()
        : this.targetLoadFactorOrDefault;
  }

  /**
   * Equivalent to {@code mathContext().orElseGet(UpdateByControl::mathContextDefault)}.
   * @see #mathContextDefault()
   */
  @Override
  public MathContext mathContextOrDefault() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.mathContextOrDefault()
        : this.mathContextOrDefault;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link UpdateByControl#useRedirection() useRedirection} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for useRedirection (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableUpdateByControl withUseRedirection(@Nullable Boolean value) {
    if (Objects.equals(this.useRedirection, value)) return this;
    return validate(new ImmutableUpdateByControl(
        value,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#chunkCapacity() chunkCapacity} attribute.
   * @param value The value for chunkCapacity
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withChunkCapacity(int value) {
    @Nullable Integer newValue = value;
    if (Objects.equals(this.chunkCapacity, newValue)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        newValue,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#chunkCapacity() chunkCapacity} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for chunkCapacity
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withChunkCapacity(OptionalInt optional) {
    @Nullable Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.chunkCapacity, value)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        value,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#maxStaticSparseMemoryOverhead() maxStaticSparseMemoryOverhead} attribute.
   * @param value The value for maxStaticSparseMemoryOverhead
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withMaxStaticSparseMemoryOverhead(double value) {
    @Nullable Double newValue = value;
    if (Objects.equals(this.maxStaticSparseMemoryOverhead, newValue)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        newValue,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#maxStaticSparseMemoryOverhead() maxStaticSparseMemoryOverhead} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for maxStaticSparseMemoryOverhead
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withMaxStaticSparseMemoryOverhead(OptionalDouble optional) {
    @Nullable Double value = optional.isPresent() ? optional.getAsDouble() : null;
    if (Objects.equals(this.maxStaticSparseMemoryOverhead, value)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        value,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#initialHashTableSize() initialHashTableSize} attribute.
   * @param value The value for initialHashTableSize
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withInitialHashTableSize(int value) {
    @Nullable Integer newValue = value;
    if (Objects.equals(this.initialHashTableSize, newValue)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        newValue,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#initialHashTableSize() initialHashTableSize} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for initialHashTableSize
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withInitialHashTableSize(OptionalInt optional) {
    @Nullable Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.initialHashTableSize, value)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        value,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#maximumLoadFactor() maximumLoadFactor} attribute.
   * @param value The value for maximumLoadFactor
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withMaximumLoadFactor(double value) {
    @Nullable Double newValue = value;
    if (Objects.equals(this.maximumLoadFactor, newValue)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        newValue,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#maximumLoadFactor() maximumLoadFactor} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for maximumLoadFactor
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withMaximumLoadFactor(OptionalDouble optional) {
    @Nullable Double value = optional.isPresent() ? optional.getAsDouble() : null;
    if (Objects.equals(this.maximumLoadFactor, value)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        value,
        this.targetLoadFactor,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#targetLoadFactor() targetLoadFactor} attribute.
   * @param value The value for targetLoadFactor
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withTargetLoadFactor(double value) {
    @Nullable Double newValue = value;
    if (Objects.equals(this.targetLoadFactor, newValue)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        newValue,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#targetLoadFactor() targetLoadFactor} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for targetLoadFactor
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withTargetLoadFactor(OptionalDouble optional) {
    @Nullable Double value = optional.isPresent() ? optional.getAsDouble() : null;
    if (Objects.equals(this.targetLoadFactor, value)) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        value,
        this.mathContext));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link UpdateByControl#mathContext() mathContext} attribute.
   * @param value The value for mathContext
   * @return A modified copy of {@code this} object
   */
  public final ImmutableUpdateByControl withMathContext(MathContext value) {
    @Nullable MathContext newValue = Objects.requireNonNull(value, "mathContext");
    if (this.mathContext == newValue) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link UpdateByControl#mathContext() mathContext} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for mathContext
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableUpdateByControl withMathContext(Optional<? extends MathContext> optional) {
    @Nullable MathContext value = optional.orElse(null);
    if (this.mathContext == value) return this;
    return validate(new ImmutableUpdateByControl(
        this.useRedirection,
        this.chunkCapacity,
        this.maxStaticSparseMemoryOverhead,
        this.initialHashTableSize,
        this.maximumLoadFactor,
        this.targetLoadFactor,
        value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableUpdateByControl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableUpdateByControl
        && equalTo(0, (ImmutableUpdateByControl) another);
  }

  private boolean equalTo(int synthetic, ImmutableUpdateByControl another) {
    return Objects.equals(useRedirection, another.useRedirection)
        && Objects.equals(chunkCapacity, another.chunkCapacity)
        && Objects.equals(maxStaticSparseMemoryOverhead, another.maxStaticSparseMemoryOverhead)
        && Objects.equals(initialHashTableSize, another.initialHashTableSize)
        && Objects.equals(maximumLoadFactor, another.maximumLoadFactor)
        && Objects.equals(targetLoadFactor, another.targetLoadFactor)
        && Objects.equals(mathContext, another.mathContext)
        && useRedirectionOrDefault == another.useRedirectionOrDefault
        && chunkCapacityOrDefault == another.chunkCapacityOrDefault
        && Double.doubleToLongBits(maxStaticSparseMemoryOverheadOrDefault) == Double.doubleToLongBits(another.maxStaticSparseMemoryOverheadOrDefault)
        && initialHashTableSizeOrDefault == another.initialHashTableSizeOrDefault
        && Double.doubleToLongBits(maximumLoadFactorOrDefault) == Double.doubleToLongBits(another.maximumLoadFactorOrDefault)
        && Double.doubleToLongBits(targetLoadFactorOrDefault) == Double.doubleToLongBits(another.targetLoadFactorOrDefault)
        && mathContextOrDefault.equals(another.mathContextOrDefault);
  }

  /**
   * Computes a hash code from attributes: {@code useRedirection}, {@code chunkCapacity}, {@code maxStaticSparseMemoryOverhead}, {@code initialHashTableSize}, {@code maximumLoadFactor}, {@code targetLoadFactor}, {@code mathContext}, {@code useRedirectionOrDefault}, {@code chunkCapacityOrDefault}, {@code maxStaticSparseMemoryOverheadOrDefault}, {@code initialHashTableSizeOrDefault}, {@code maximumLoadFactorOrDefault}, {@code targetLoadFactorOrDefault}, {@code mathContextOrDefault}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(useRedirection);
    h += (h << 5) + Objects.hashCode(chunkCapacity);
    h += (h << 5) + Objects.hashCode(maxStaticSparseMemoryOverhead);
    h += (h << 5) + Objects.hashCode(initialHashTableSize);
    h += (h << 5) + Objects.hashCode(maximumLoadFactor);
    h += (h << 5) + Objects.hashCode(targetLoadFactor);
    h += (h << 5) + Objects.hashCode(mathContext);
    h += (h << 5) + Boolean.hashCode(useRedirectionOrDefault);
    h += (h << 5) + chunkCapacityOrDefault;
    h += (h << 5) + Double.hashCode(maxStaticSparseMemoryOverheadOrDefault);
    h += (h << 5) + initialHashTableSizeOrDefault;
    h += (h << 5) + Double.hashCode(maximumLoadFactorOrDefault);
    h += (h << 5) + Double.hashCode(targetLoadFactorOrDefault);
    h += (h << 5) + mathContextOrDefault.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code UpdateByControl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("UpdateByControl{");
    if (useRedirection != null) {
      builder.append("useRedirection=").append(useRedirection);
    }
    if (chunkCapacity != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("chunkCapacity=").append(chunkCapacity);
    }
    if (maxStaticSparseMemoryOverhead != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("maxStaticSparseMemoryOverhead=").append(maxStaticSparseMemoryOverhead);
    }
    if (initialHashTableSize != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("initialHashTableSize=").append(initialHashTableSize);
    }
    if (maximumLoadFactor != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("maximumLoadFactor=").append(maximumLoadFactor);
    }
    if (targetLoadFactor != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("targetLoadFactor=").append(targetLoadFactor);
    }
    if (mathContext != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("mathContext=").append(mathContext);
    }
    if (builder.length() > 16) builder.append(", ");
    builder.append("useRedirectionOrDefault=").append(useRedirectionOrDefault);
    builder.append(", ");
    builder.append("chunkCapacityOrDefault=").append(chunkCapacityOrDefault);
    builder.append(", ");
    builder.append("maxStaticSparseMemoryOverheadOrDefault=").append(maxStaticSparseMemoryOverheadOrDefault);
    builder.append(", ");
    builder.append("initialHashTableSizeOrDefault=").append(initialHashTableSizeOrDefault);
    builder.append(", ");
    builder.append("maximumLoadFactorOrDefault=").append(maximumLoadFactorOrDefault);
    builder.append(", ");
    builder.append("targetLoadFactorOrDefault=").append(targetLoadFactorOrDefault);
    builder.append(", ");
    builder.append("mathContextOrDefault=").append(mathContextOrDefault);
    return builder.append("}").toString();
  }

  private static ImmutableUpdateByControl validate(ImmutableUpdateByControl instance) {
    instance.checkTargetLTEMaximum();
    instance.checkTargetLoadFactor();
    instance.checkMaximumLoadFactor();
    instance.checkInitialHashTableSize();
    instance.checkChunkCapacity();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link UpdateByControl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable UpdateByControl instance
   */
  public static ImmutableUpdateByControl copyOf(UpdateByControl instance) {
    if (instance instanceof ImmutableUpdateByControl) {
      return (ImmutableUpdateByControl) instance;
    }
    return ImmutableUpdateByControl.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableUpdateByControl ImmutableUpdateByControl}.
   * <pre>
   * ImmutableUpdateByControl.builder()
   *    .useRedirection(Boolean | null) // nullable {@link UpdateByControl#useRedirection() useRedirection}
   *    .chunkCapacity(int) // optional {@link UpdateByControl#chunkCapacity() chunkCapacity}
   *    .maxStaticSparseMemoryOverhead(double) // optional {@link UpdateByControl#maxStaticSparseMemoryOverhead() maxStaticSparseMemoryOverhead}
   *    .initialHashTableSize(int) // optional {@link UpdateByControl#initialHashTableSize() initialHashTableSize}
   *    .maximumLoadFactor(double) // optional {@link UpdateByControl#maximumLoadFactor() maximumLoadFactor}
   *    .targetLoadFactor(double) // optional {@link UpdateByControl#targetLoadFactor() targetLoadFactor}
   *    .mathContext(java.math.MathContext) // optional {@link UpdateByControl#mathContext() mathContext}
   *    .build();
   * </pre>
   * @return A new ImmutableUpdateByControl builder
   */
  public static ImmutableUpdateByControl.Builder builder() {
    return new ImmutableUpdateByControl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableUpdateByControl ImmutableUpdateByControl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "UpdateByControl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements UpdateByControl.Builder {
    private @Nullable Boolean useRedirection;
    private @Nullable Integer chunkCapacity;
    private @Nullable Double maxStaticSparseMemoryOverhead;
    private @Nullable Integer initialHashTableSize;
    private @Nullable Double maximumLoadFactor;
    private @Nullable Double targetLoadFactor;
    private @Nullable MathContext mathContext;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code UpdateByControl} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(UpdateByControl instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable Boolean useRedirectionValue = instance.useRedirection();
      if (useRedirectionValue != null) {
        useRedirection(useRedirectionValue);
      }
      OptionalInt chunkCapacityOptional = instance.chunkCapacity();
      if (chunkCapacityOptional.isPresent()) {
        chunkCapacity(chunkCapacityOptional);
      }
      OptionalDouble maxStaticSparseMemoryOverheadOptional = instance.maxStaticSparseMemoryOverhead();
      if (maxStaticSparseMemoryOverheadOptional.isPresent()) {
        maxStaticSparseMemoryOverhead(maxStaticSparseMemoryOverheadOptional);
      }
      OptionalInt initialHashTableSizeOptional = instance.initialHashTableSize();
      if (initialHashTableSizeOptional.isPresent()) {
        initialHashTableSize(initialHashTableSizeOptional);
      }
      OptionalDouble maximumLoadFactorOptional = instance.maximumLoadFactor();
      if (maximumLoadFactorOptional.isPresent()) {
        maximumLoadFactor(maximumLoadFactorOptional);
      }
      OptionalDouble targetLoadFactorOptional = instance.targetLoadFactor();
      if (targetLoadFactorOptional.isPresent()) {
        targetLoadFactor(targetLoadFactorOptional);
      }
      Optional<MathContext> mathContextOptional = instance.mathContext();
      if (mathContextOptional.isPresent()) {
        mathContext(mathContextOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link UpdateByControl#useRedirection() useRedirection} attribute.
     * @param useRedirection The value for useRedirection (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder useRedirection(@Nullable Boolean useRedirection) {
      this.useRedirection = useRedirection;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#chunkCapacity() chunkCapacity} to chunkCapacity.
     * @param chunkCapacity The value for chunkCapacity
     * @return {@code this} builder for chained invocation
     */
    public final Builder chunkCapacity(int chunkCapacity) {
      this.chunkCapacity = chunkCapacity;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#chunkCapacity() chunkCapacity} to chunkCapacity.
     * @param chunkCapacity The value for chunkCapacity
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder chunkCapacity(OptionalInt chunkCapacity) {
      this.chunkCapacity = chunkCapacity.isPresent() ? chunkCapacity.getAsInt() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#maxStaticSparseMemoryOverhead() maxStaticSparseMemoryOverhead} to maxStaticSparseMemoryOverhead.
     * @param maxStaticSparseMemoryOverhead The value for maxStaticSparseMemoryOverhead
     * @return {@code this} builder for chained invocation
     */
    public final Builder maxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead) {
      this.maxStaticSparseMemoryOverhead = maxStaticSparseMemoryOverhead;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#maxStaticSparseMemoryOverhead() maxStaticSparseMemoryOverhead} to maxStaticSparseMemoryOverhead.
     * @param maxStaticSparseMemoryOverhead The value for maxStaticSparseMemoryOverhead
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder maxStaticSparseMemoryOverhead(OptionalDouble maxStaticSparseMemoryOverhead) {
      this.maxStaticSparseMemoryOverhead = maxStaticSparseMemoryOverhead.isPresent() ? maxStaticSparseMemoryOverhead.getAsDouble() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#initialHashTableSize() initialHashTableSize} to initialHashTableSize.
     * @param initialHashTableSize The value for initialHashTableSize
     * @return {@code this} builder for chained invocation
     */
    public final Builder initialHashTableSize(int initialHashTableSize) {
      this.initialHashTableSize = initialHashTableSize;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#initialHashTableSize() initialHashTableSize} to initialHashTableSize.
     * @param initialHashTableSize The value for initialHashTableSize
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder initialHashTableSize(OptionalInt initialHashTableSize) {
      this.initialHashTableSize = initialHashTableSize.isPresent() ? initialHashTableSize.getAsInt() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#maximumLoadFactor() maximumLoadFactor} to maximumLoadFactor.
     * @param maximumLoadFactor The value for maximumLoadFactor
     * @return {@code this} builder for chained invocation
     */
    public final Builder maximumLoadFactor(double maximumLoadFactor) {
      this.maximumLoadFactor = maximumLoadFactor;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#maximumLoadFactor() maximumLoadFactor} to maximumLoadFactor.
     * @param maximumLoadFactor The value for maximumLoadFactor
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder maximumLoadFactor(OptionalDouble maximumLoadFactor) {
      this.maximumLoadFactor = maximumLoadFactor.isPresent() ? maximumLoadFactor.getAsDouble() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#targetLoadFactor() targetLoadFactor} to targetLoadFactor.
     * @param targetLoadFactor The value for targetLoadFactor
     * @return {@code this} builder for chained invocation
     */
    public final Builder targetLoadFactor(double targetLoadFactor) {
      this.targetLoadFactor = targetLoadFactor;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#targetLoadFactor() targetLoadFactor} to targetLoadFactor.
     * @param targetLoadFactor The value for targetLoadFactor
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder targetLoadFactor(OptionalDouble targetLoadFactor) {
      this.targetLoadFactor = targetLoadFactor.isPresent() ? targetLoadFactor.getAsDouble() : null;
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#mathContext() mathContext} to mathContext.
     * @param mathContext The value for mathContext
     * @return {@code this} builder for chained invocation
     */
    public final Builder mathContext(MathContext mathContext) {
      this.mathContext = Objects.requireNonNull(mathContext, "mathContext");
      return this;
    }

    /**
     * Initializes the optional value {@link UpdateByControl#mathContext() mathContext} to mathContext.
     * @param mathContext The value for mathContext
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder mathContext(Optional<? extends MathContext> mathContext) {
      this.mathContext = mathContext.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableUpdateByControl ImmutableUpdateByControl}.
     * @return An immutable instance of UpdateByControl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableUpdateByControl build() {
      return ImmutableUpdateByControl.validate(new ImmutableUpdateByControl(
          useRedirection,
          chunkCapacity,
          maxStaticSparseMemoryOverhead,
          initialHashTableSize,
          maximumLoadFactor,
          targetLoadFactor,
          mathContext));
    }
  }
}
