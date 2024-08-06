package io.deephaven.extensions.barrage;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link BarrageSnapshotOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBarrageSnapshotOptions.builder()}.
 */
@Generated(from = "BarrageSnapshotOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableBarrageSnapshotOptions
    extends BarrageSnapshotOptions {
  private final boolean useDeephavenNulls;
  private final int batchSize;
  private final int maxMessageSize;
  private final ColumnConversionMode columnConversionMode;

  private ImmutableBarrageSnapshotOptions(ImmutableBarrageSnapshotOptions.Builder builder) {
    if (builder.useDeephavenNullsIsSet()) {
      initShim.useDeephavenNulls(builder.useDeephavenNulls);
    }
    if (builder.batchSizeIsSet()) {
      initShim.batchSize(builder.batchSize);
    }
    if (builder.maxMessageSizeIsSet()) {
      initShim.maxMessageSize(builder.maxMessageSize);
    }
    if (builder.columnConversionMode != null) {
      initShim.columnConversionMode(builder.columnConversionMode);
    }
    this.useDeephavenNulls = initShim.useDeephavenNulls();
    this.batchSize = initShim.batchSize();
    this.maxMessageSize = initShim.maxMessageSize();
    this.columnConversionMode = initShim.columnConversionMode();
    this.initShim = null;
  }

  private ImmutableBarrageSnapshotOptions(
      boolean useDeephavenNulls,
      int batchSize,
      int maxMessageSize,
      ColumnConversionMode columnConversionMode) {
    this.useDeephavenNulls = useDeephavenNulls;
    this.batchSize = batchSize;
    this.maxMessageSize = maxMessageSize;
    this.columnConversionMode = columnConversionMode;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "BarrageSnapshotOptions", generator = "Immutables")
  private final class InitShim {
    private byte useDeephavenNullsBuildStage = STAGE_UNINITIALIZED;
    private boolean useDeephavenNulls;

    boolean useDeephavenNulls() {
      if (useDeephavenNullsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (useDeephavenNullsBuildStage == STAGE_UNINITIALIZED) {
        useDeephavenNullsBuildStage = STAGE_INITIALIZING;
        this.useDeephavenNulls = ImmutableBarrageSnapshotOptions.super.useDeephavenNulls();
        useDeephavenNullsBuildStage = STAGE_INITIALIZED;
      }
      return this.useDeephavenNulls;
    }

    void useDeephavenNulls(boolean useDeephavenNulls) {
      this.useDeephavenNulls = useDeephavenNulls;
      useDeephavenNullsBuildStage = STAGE_INITIALIZED;
    }

    private byte batchSizeBuildStage = STAGE_UNINITIALIZED;
    private int batchSize;

    int batchSize() {
      if (batchSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (batchSizeBuildStage == STAGE_UNINITIALIZED) {
        batchSizeBuildStage = STAGE_INITIALIZING;
        this.batchSize = ImmutableBarrageSnapshotOptions.super.batchSize();
        batchSizeBuildStage = STAGE_INITIALIZED;
      }
      return this.batchSize;
    }

    void batchSize(int batchSize) {
      this.batchSize = batchSize;
      batchSizeBuildStage = STAGE_INITIALIZED;
    }

    private byte maxMessageSizeBuildStage = STAGE_UNINITIALIZED;
    private int maxMessageSize;

    int maxMessageSize() {
      if (maxMessageSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maxMessageSizeBuildStage == STAGE_UNINITIALIZED) {
        maxMessageSizeBuildStage = STAGE_INITIALIZING;
        this.maxMessageSize = ImmutableBarrageSnapshotOptions.super.maxMessageSize();
        maxMessageSizeBuildStage = STAGE_INITIALIZED;
      }
      return this.maxMessageSize;
    }

    void maxMessageSize(int maxMessageSize) {
      this.maxMessageSize = maxMessageSize;
      maxMessageSizeBuildStage = STAGE_INITIALIZED;
    }

    private byte columnConversionModeBuildStage = STAGE_UNINITIALIZED;
    private ColumnConversionMode columnConversionMode;

    ColumnConversionMode columnConversionMode() {
      if (columnConversionModeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (columnConversionModeBuildStage == STAGE_UNINITIALIZED) {
        columnConversionModeBuildStage = STAGE_INITIALIZING;
        this.columnConversionMode = Objects.requireNonNull(ImmutableBarrageSnapshotOptions.super.columnConversionMode(), "columnConversionMode");
        columnConversionModeBuildStage = STAGE_INITIALIZED;
      }
      return this.columnConversionMode;
    }

    void columnConversionMode(ColumnConversionMode columnConversionMode) {
      this.columnConversionMode = columnConversionMode;
      columnConversionModeBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (useDeephavenNullsBuildStage == STAGE_INITIALIZING) attributes.add("useDeephavenNulls");
      if (batchSizeBuildStage == STAGE_INITIALIZING) attributes.add("batchSize");
      if (maxMessageSizeBuildStage == STAGE_INITIALIZING) attributes.add("maxMessageSize");
      if (columnConversionModeBuildStage == STAGE_INITIALIZING) attributes.add("columnConversionMode");
      return "Cannot build BarrageSnapshotOptions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * By default, prefer to communicate null values using the arrow-compatible validity structure.
   * @return whether to use deephaven nulls
   */
  @Override
  public boolean useDeephavenNulls() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.useDeephavenNulls()
        : this.useDeephavenNulls;
  }

  /**
   * @return the preferred batch size if specified
   */
  @Override
  public int batchSize() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.batchSize()
        : this.batchSize;
  }

  /**
   * @return the maximum GRPC message size if specified
   */
  @Override
  public int maxMessageSize() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.maxMessageSize()
        : this.maxMessageSize;
  }

  /**
   * @return The value of the {@code columnConversionMode} attribute
   */
  @Override
  public ColumnConversionMode columnConversionMode() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.columnConversionMode()
        : this.columnConversionMode;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSnapshotOptions#useDeephavenNulls() useDeephavenNulls} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for useDeephavenNulls
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSnapshotOptions withUseDeephavenNulls(boolean value) {
    if (this.useDeephavenNulls == value) return this;
    return new ImmutableBarrageSnapshotOptions(value, this.batchSize, this.maxMessageSize, this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSnapshotOptions#batchSize() batchSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for batchSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSnapshotOptions withBatchSize(int value) {
    if (this.batchSize == value) return this;
    return new ImmutableBarrageSnapshotOptions(this.useDeephavenNulls, value, this.maxMessageSize, this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSnapshotOptions#maxMessageSize() maxMessageSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxMessageSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSnapshotOptions withMaxMessageSize(int value) {
    if (this.maxMessageSize == value) return this;
    return new ImmutableBarrageSnapshotOptions(this.useDeephavenNulls, this.batchSize, value, this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSnapshotOptions#columnConversionMode() columnConversionMode} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnConversionMode
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSnapshotOptions withColumnConversionMode(ColumnConversionMode value) {
    ColumnConversionMode newValue = Objects.requireNonNull(value, "columnConversionMode");
    if (this.columnConversionMode == newValue) return this;
    return new ImmutableBarrageSnapshotOptions(this.useDeephavenNulls, this.batchSize, this.maxMessageSize, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBarrageSnapshotOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBarrageSnapshotOptions
        && equalTo(0, (ImmutableBarrageSnapshotOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableBarrageSnapshotOptions another) {
    return useDeephavenNulls == another.useDeephavenNulls
        && batchSize == another.batchSize
        && maxMessageSize == another.maxMessageSize
        && columnConversionMode.equals(another.columnConversionMode);
  }

  /**
   * Computes a hash code from attributes: {@code useDeephavenNulls}, {@code batchSize}, {@code maxMessageSize}, {@code columnConversionMode}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Booleans.hashCode(useDeephavenNulls);
    h += (h << 5) + batchSize;
    h += (h << 5) + maxMessageSize;
    h += (h << 5) + columnConversionMode.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code BarrageSnapshotOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("BarrageSnapshotOptions")
        .omitNullValues()
        .add("useDeephavenNulls", useDeephavenNulls)
        .add("batchSize", batchSize)
        .add("maxMessageSize", maxMessageSize)
        .add("columnConversionMode", columnConversionMode)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link BarrageSnapshotOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BarrageSnapshotOptions instance
   */
  public static ImmutableBarrageSnapshotOptions copyOf(BarrageSnapshotOptions instance) {
    if (instance instanceof ImmutableBarrageSnapshotOptions) {
      return (ImmutableBarrageSnapshotOptions) instance;
    }
    return ImmutableBarrageSnapshotOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBarrageSnapshotOptions ImmutableBarrageSnapshotOptions}.
   * <pre>
   * ImmutableBarrageSnapshotOptions.builder()
   *    .useDeephavenNulls(boolean) // optional {@link BarrageSnapshotOptions#useDeephavenNulls() useDeephavenNulls}
   *    .batchSize(int) // optional {@link BarrageSnapshotOptions#batchSize() batchSize}
   *    .maxMessageSize(int) // optional {@link BarrageSnapshotOptions#maxMessageSize() maxMessageSize}
   *    .columnConversionMode(io.deephaven.extensions.barrage.ColumnConversionMode) // optional {@link BarrageSnapshotOptions#columnConversionMode() columnConversionMode}
   *    .build();
   * </pre>
   * @return A new ImmutableBarrageSnapshotOptions builder
   */
  public static ImmutableBarrageSnapshotOptions.Builder builder() {
    return new ImmutableBarrageSnapshotOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBarrageSnapshotOptions ImmutableBarrageSnapshotOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BarrageSnapshotOptions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements BarrageSnapshotOptions.Builder {
    private static final long OPT_BIT_USE_DEEPHAVEN_NULLS = 0x1L;
    private static final long OPT_BIT_BATCH_SIZE = 0x2L;
    private static final long OPT_BIT_MAX_MESSAGE_SIZE = 0x4L;
    private long optBits;

    private boolean useDeephavenNulls;
    private int batchSize;
    private int maxMessageSize;
    private @Nullable ColumnConversionMode columnConversionMode;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.extensions.barrage.util.StreamReaderOptions} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(StreamReaderOptions instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.extensions.barrage.BarrageSnapshotOptions} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(BarrageSnapshotOptions instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      if (object instanceof StreamReaderOptions) {
        StreamReaderOptions instance = (StreamReaderOptions) object;
        columnConversionMode(instance.columnConversionMode());
        batchSize(instance.batchSize());
        maxMessageSize(instance.maxMessageSize());
        useDeephavenNulls(instance.useDeephavenNulls());
      }
    }

    /**
     * Initializes the value for the {@link BarrageSnapshotOptions#useDeephavenNulls() useDeephavenNulls} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSnapshotOptions#useDeephavenNulls() useDeephavenNulls}.</em>
     * @param useDeephavenNulls The value for useDeephavenNulls 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder useDeephavenNulls(boolean useDeephavenNulls) {
      this.useDeephavenNulls = useDeephavenNulls;
      optBits |= OPT_BIT_USE_DEEPHAVEN_NULLS;
      return this;
    }

    /**
     * Initializes the value for the {@link BarrageSnapshotOptions#batchSize() batchSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSnapshotOptions#batchSize() batchSize}.</em>
     * @param batchSize The value for batchSize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      optBits |= OPT_BIT_BATCH_SIZE;
      return this;
    }

    /**
     * Initializes the value for the {@link BarrageSnapshotOptions#maxMessageSize() maxMessageSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSnapshotOptions#maxMessageSize() maxMessageSize}.</em>
     * @param maxMessageSize The value for maxMessageSize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder maxMessageSize(int maxMessageSize) {
      this.maxMessageSize = maxMessageSize;
      optBits |= OPT_BIT_MAX_MESSAGE_SIZE;
      return this;
    }

    /**
     * Initializes the value for the {@link BarrageSnapshotOptions#columnConversionMode() columnConversionMode} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSnapshotOptions#columnConversionMode() columnConversionMode}.</em>
     * @param columnConversionMode The value for columnConversionMode 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnConversionMode(ColumnConversionMode columnConversionMode) {
      this.columnConversionMode = Objects.requireNonNull(columnConversionMode, "columnConversionMode");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBarrageSnapshotOptions ImmutableBarrageSnapshotOptions}.
     * @return An immutable instance of BarrageSnapshotOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBarrageSnapshotOptions build() {
      return new ImmutableBarrageSnapshotOptions(this);
    }

    private boolean useDeephavenNullsIsSet() {
      return (optBits & OPT_BIT_USE_DEEPHAVEN_NULLS) != 0;
    }

    private boolean batchSizeIsSet() {
      return (optBits & OPT_BIT_BATCH_SIZE) != 0;
    }

    private boolean maxMessageSizeIsSet() {
      return (optBits & OPT_BIT_MAX_MESSAGE_SIZE) != 0;
    }
  }
}
