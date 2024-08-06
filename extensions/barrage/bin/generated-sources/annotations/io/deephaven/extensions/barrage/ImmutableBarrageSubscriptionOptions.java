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
 * Immutable implementation of {@link BarrageSubscriptionOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBarrageSubscriptionOptions.builder()}.
 */
@Generated(from = "BarrageSubscriptionOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableBarrageSubscriptionOptions
    extends BarrageSubscriptionOptions {
  private final boolean useDeephavenNulls;
  private final boolean columnsAsList;
  private final int minUpdateIntervalMs;
  private final int batchSize;
  private final int maxMessageSize;
  private final ColumnConversionMode columnConversionMode;

  private ImmutableBarrageSubscriptionOptions(ImmutableBarrageSubscriptionOptions.Builder builder) {
    if (builder.useDeephavenNullsIsSet()) {
      initShim.useDeephavenNulls(builder.useDeephavenNulls);
    }
    if (builder.columnsAsListIsSet()) {
      initShim.columnsAsList(builder.columnsAsList);
    }
    if (builder.minUpdateIntervalMsIsSet()) {
      initShim.minUpdateIntervalMs(builder.minUpdateIntervalMs);
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
    this.columnsAsList = initShim.columnsAsList();
    this.minUpdateIntervalMs = initShim.minUpdateIntervalMs();
    this.batchSize = initShim.batchSize();
    this.maxMessageSize = initShim.maxMessageSize();
    this.columnConversionMode = initShim.columnConversionMode();
    this.initShim = null;
  }

  private ImmutableBarrageSubscriptionOptions(
      boolean useDeephavenNulls,
      boolean columnsAsList,
      int minUpdateIntervalMs,
      int batchSize,
      int maxMessageSize,
      ColumnConversionMode columnConversionMode) {
    this.useDeephavenNulls = useDeephavenNulls;
    this.columnsAsList = columnsAsList;
    this.minUpdateIntervalMs = minUpdateIntervalMs;
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

  @Generated(from = "BarrageSubscriptionOptions", generator = "Immutables")
  private final class InitShim {
    private byte useDeephavenNullsBuildStage = STAGE_UNINITIALIZED;
    private boolean useDeephavenNulls;

    boolean useDeephavenNulls() {
      if (useDeephavenNullsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (useDeephavenNullsBuildStage == STAGE_UNINITIALIZED) {
        useDeephavenNullsBuildStage = STAGE_INITIALIZING;
        this.useDeephavenNulls = ImmutableBarrageSubscriptionOptions.super.useDeephavenNulls();
        useDeephavenNullsBuildStage = STAGE_INITIALIZED;
      }
      return this.useDeephavenNulls;
    }

    void useDeephavenNulls(boolean useDeephavenNulls) {
      this.useDeephavenNulls = useDeephavenNulls;
      useDeephavenNullsBuildStage = STAGE_INITIALIZED;
    }

    private byte columnsAsListBuildStage = STAGE_UNINITIALIZED;
    private boolean columnsAsList;

    boolean columnsAsList() {
      if (columnsAsListBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (columnsAsListBuildStage == STAGE_UNINITIALIZED) {
        columnsAsListBuildStage = STAGE_INITIALIZING;
        this.columnsAsList = ImmutableBarrageSubscriptionOptions.super.columnsAsList();
        columnsAsListBuildStage = STAGE_INITIALIZED;
      }
      return this.columnsAsList;
    }

    void columnsAsList(boolean columnsAsList) {
      this.columnsAsList = columnsAsList;
      columnsAsListBuildStage = STAGE_INITIALIZED;
    }

    private byte minUpdateIntervalMsBuildStage = STAGE_UNINITIALIZED;
    private int minUpdateIntervalMs;

    int minUpdateIntervalMs() {
      if (minUpdateIntervalMsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (minUpdateIntervalMsBuildStage == STAGE_UNINITIALIZED) {
        minUpdateIntervalMsBuildStage = STAGE_INITIALIZING;
        this.minUpdateIntervalMs = ImmutableBarrageSubscriptionOptions.super.minUpdateIntervalMs();
        minUpdateIntervalMsBuildStage = STAGE_INITIALIZED;
      }
      return this.minUpdateIntervalMs;
    }

    void minUpdateIntervalMs(int minUpdateIntervalMs) {
      this.minUpdateIntervalMs = minUpdateIntervalMs;
      minUpdateIntervalMsBuildStage = STAGE_INITIALIZED;
    }

    private byte batchSizeBuildStage = STAGE_UNINITIALIZED;
    private int batchSize;

    int batchSize() {
      if (batchSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (batchSizeBuildStage == STAGE_UNINITIALIZED) {
        batchSizeBuildStage = STAGE_INITIALIZING;
        this.batchSize = ImmutableBarrageSubscriptionOptions.super.batchSize();
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
        this.maxMessageSize = ImmutableBarrageSubscriptionOptions.super.maxMessageSize();
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
        this.columnConversionMode = Objects.requireNonNull(ImmutableBarrageSubscriptionOptions.super.columnConversionMode(), "columnConversionMode");
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
      if (columnsAsListBuildStage == STAGE_INITIALIZING) attributes.add("columnsAsList");
      if (minUpdateIntervalMsBuildStage == STAGE_INITIALIZING) attributes.add("minUpdateIntervalMs");
      if (batchSizeBuildStage == STAGE_INITIALIZING) attributes.add("batchSize");
      if (maxMessageSizeBuildStage == STAGE_INITIALIZING) attributes.add("maxMessageSize");
      if (columnConversionModeBuildStage == STAGE_INITIALIZING) attributes.add("columnConversionMode");
      return "Cannot build BarrageSubscriptionOptions, attribute initializers form cycle " + attributes;
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
   * Requesting clients can specify whether they want columns to be returned wrapped in a list. This enables easier
   * support in some official arrow clients, but is not the default.
   */
  @Override
  public boolean columnsAsList() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.columnsAsList()
        : this.columnsAsList;
  }

  /**
   * By default, we should not specify anything; the server will use whatever it is configured with. If multiple
   * subscriptions exist on a table (via the same client or via multiple clients) then the server will re-use state
   * needed to perform barrage-acrobatics for both of them. This greatly reduces the burden each client adds to the
   * server's workload. If a given table does want a shorter interval, consider using that shorter interval for all
   * subscriptions to that table.
   * The default interval can be set on the server with the flag
   * {@code io.deephaven.server.arrow.ArrowFlightUtil#DEFAULT_UPDATE_INTERVAL_MS}, or
   * {@code -Dbarrage.minUpdateInterval=1000}.
   * Related, when shortening the minUpdateInterval, you typically want to shorten the server's UGP cycle enough to
   * update at least as quickly. This can be done on the server with the flag
   * {@code io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph#defaultTargetCycleTime}, or
   * {@code -DPeriodicUpdateGraph.targetcycletime=1000}.
   * @return the update interval to subscribe for
   */
  @Override
  public int minUpdateIntervalMs() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.minUpdateIntervalMs()
        : this.minUpdateIntervalMs;
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
   * @return the preferred maximum GRPC message size if specified
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
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#useDeephavenNulls() useDeephavenNulls} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for useDeephavenNulls
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withUseDeephavenNulls(boolean value) {
    if (this.useDeephavenNulls == value) return this;
    return new ImmutableBarrageSubscriptionOptions(
        value,
        this.columnsAsList,
        this.minUpdateIntervalMs,
        this.batchSize,
        this.maxMessageSize,
        this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#columnsAsList() columnsAsList} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnsAsList
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withColumnsAsList(boolean value) {
    if (this.columnsAsList == value) return this;
    return new ImmutableBarrageSubscriptionOptions(
        this.useDeephavenNulls,
        value,
        this.minUpdateIntervalMs,
        this.batchSize,
        this.maxMessageSize,
        this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#minUpdateIntervalMs() minUpdateIntervalMs} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for minUpdateIntervalMs
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withMinUpdateIntervalMs(int value) {
    if (this.minUpdateIntervalMs == value) return this;
    return new ImmutableBarrageSubscriptionOptions(
        this.useDeephavenNulls,
        this.columnsAsList,
        value,
        this.batchSize,
        this.maxMessageSize,
        this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#batchSize() batchSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for batchSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withBatchSize(int value) {
    if (this.batchSize == value) return this;
    return new ImmutableBarrageSubscriptionOptions(
        this.useDeephavenNulls,
        this.columnsAsList,
        this.minUpdateIntervalMs,
        value,
        this.maxMessageSize,
        this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#maxMessageSize() maxMessageSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxMessageSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withMaxMessageSize(int value) {
    if (this.maxMessageSize == value) return this;
    return new ImmutableBarrageSubscriptionOptions(
        this.useDeephavenNulls,
        this.columnsAsList,
        this.minUpdateIntervalMs,
        this.batchSize,
        value,
        this.columnConversionMode);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BarrageSubscriptionOptions#columnConversionMode() columnConversionMode} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for columnConversionMode
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBarrageSubscriptionOptions withColumnConversionMode(ColumnConversionMode value) {
    ColumnConversionMode newValue = Objects.requireNonNull(value, "columnConversionMode");
    if (this.columnConversionMode == newValue) return this;
    return new ImmutableBarrageSubscriptionOptions(
        this.useDeephavenNulls,
        this.columnsAsList,
        this.minUpdateIntervalMs,
        this.batchSize,
        this.maxMessageSize,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBarrageSubscriptionOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBarrageSubscriptionOptions
        && equalTo(0, (ImmutableBarrageSubscriptionOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableBarrageSubscriptionOptions another) {
    return useDeephavenNulls == another.useDeephavenNulls
        && columnsAsList == another.columnsAsList
        && minUpdateIntervalMs == another.minUpdateIntervalMs
        && batchSize == another.batchSize
        && maxMessageSize == another.maxMessageSize
        && columnConversionMode.equals(another.columnConversionMode);
  }

  /**
   * Computes a hash code from attributes: {@code useDeephavenNulls}, {@code columnsAsList}, {@code minUpdateIntervalMs}, {@code batchSize}, {@code maxMessageSize}, {@code columnConversionMode}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Booleans.hashCode(useDeephavenNulls);
    h += (h << 5) + Booleans.hashCode(columnsAsList);
    h += (h << 5) + minUpdateIntervalMs;
    h += (h << 5) + batchSize;
    h += (h << 5) + maxMessageSize;
    h += (h << 5) + columnConversionMode.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code BarrageSubscriptionOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("BarrageSubscriptionOptions")
        .omitNullValues()
        .add("useDeephavenNulls", useDeephavenNulls)
        .add("columnsAsList", columnsAsList)
        .add("minUpdateIntervalMs", minUpdateIntervalMs)
        .add("batchSize", batchSize)
        .add("maxMessageSize", maxMessageSize)
        .add("columnConversionMode", columnConversionMode)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link BarrageSubscriptionOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BarrageSubscriptionOptions instance
   */
  public static ImmutableBarrageSubscriptionOptions copyOf(BarrageSubscriptionOptions instance) {
    if (instance instanceof ImmutableBarrageSubscriptionOptions) {
      return (ImmutableBarrageSubscriptionOptions) instance;
    }
    return ImmutableBarrageSubscriptionOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBarrageSubscriptionOptions ImmutableBarrageSubscriptionOptions}.
   * <pre>
   * ImmutableBarrageSubscriptionOptions.builder()
   *    .useDeephavenNulls(boolean) // optional {@link BarrageSubscriptionOptions#useDeephavenNulls() useDeephavenNulls}
   *    .columnsAsList(boolean) // optional {@link BarrageSubscriptionOptions#columnsAsList() columnsAsList}
   *    .minUpdateIntervalMs(int) // optional {@link BarrageSubscriptionOptions#minUpdateIntervalMs() minUpdateIntervalMs}
   *    .batchSize(int) // optional {@link BarrageSubscriptionOptions#batchSize() batchSize}
   *    .maxMessageSize(int) // optional {@link BarrageSubscriptionOptions#maxMessageSize() maxMessageSize}
   *    .columnConversionMode(io.deephaven.extensions.barrage.ColumnConversionMode) // optional {@link BarrageSubscriptionOptions#columnConversionMode() columnConversionMode}
   *    .build();
   * </pre>
   * @return A new ImmutableBarrageSubscriptionOptions builder
   */
  public static ImmutableBarrageSubscriptionOptions.Builder builder() {
    return new ImmutableBarrageSubscriptionOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBarrageSubscriptionOptions ImmutableBarrageSubscriptionOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BarrageSubscriptionOptions", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements BarrageSubscriptionOptions.Builder {
    private static final long OPT_BIT_USE_DEEPHAVEN_NULLS = 0x1L;
    private static final long OPT_BIT_COLUMNS_AS_LIST = 0x2L;
    private static final long OPT_BIT_MIN_UPDATE_INTERVAL_MS = 0x4L;
    private static final long OPT_BIT_BATCH_SIZE = 0x8L;
    private static final long OPT_BIT_MAX_MESSAGE_SIZE = 0x10L;
    private long optBits;

    private boolean useDeephavenNulls;
    private boolean columnsAsList;
    private int minUpdateIntervalMs;
    private int batchSize;
    private int maxMessageSize;
    private @Nullable ColumnConversionMode columnConversionMode;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.extensions.barrage.BarrageSubscriptionOptions} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(BarrageSubscriptionOptions instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
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

    private void from(Object object) {
      if (object instanceof BarrageSubscriptionOptions) {
        BarrageSubscriptionOptions instance = (BarrageSubscriptionOptions) object;
        minUpdateIntervalMs(instance.minUpdateIntervalMs());
      }
      if (object instanceof StreamReaderOptions) {
        StreamReaderOptions instance = (StreamReaderOptions) object;
        columnConversionMode(instance.columnConversionMode());
        batchSize(instance.batchSize());
        maxMessageSize(instance.maxMessageSize());
        useDeephavenNulls(instance.useDeephavenNulls());
        columnsAsList(instance.columnsAsList());
      }
    }

    /**
     * Initializes the value for the {@link BarrageSubscriptionOptions#useDeephavenNulls() useDeephavenNulls} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#useDeephavenNulls() useDeephavenNulls}.</em>
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
     * Initializes the value for the {@link BarrageSubscriptionOptions#columnsAsList() columnsAsList} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#columnsAsList() columnsAsList}.</em>
     * @param columnsAsList The value for columnsAsList 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnsAsList(boolean columnsAsList) {
      this.columnsAsList = columnsAsList;
      optBits |= OPT_BIT_COLUMNS_AS_LIST;
      return this;
    }

    /**
     * Initializes the value for the {@link BarrageSubscriptionOptions#minUpdateIntervalMs() minUpdateIntervalMs} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#minUpdateIntervalMs() minUpdateIntervalMs}.</em>
     * @param minUpdateIntervalMs The value for minUpdateIntervalMs 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder minUpdateIntervalMs(int minUpdateIntervalMs) {
      this.minUpdateIntervalMs = minUpdateIntervalMs;
      optBits |= OPT_BIT_MIN_UPDATE_INTERVAL_MS;
      return this;
    }

    /**
     * Initializes the value for the {@link BarrageSubscriptionOptions#batchSize() batchSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#batchSize() batchSize}.</em>
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
     * Initializes the value for the {@link BarrageSubscriptionOptions#maxMessageSize() maxMessageSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#maxMessageSize() maxMessageSize}.</em>
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
     * Initializes the value for the {@link BarrageSubscriptionOptions#columnConversionMode() columnConversionMode} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link BarrageSubscriptionOptions#columnConversionMode() columnConversionMode}.</em>
     * @param columnConversionMode The value for columnConversionMode 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder columnConversionMode(ColumnConversionMode columnConversionMode) {
      this.columnConversionMode = Objects.requireNonNull(columnConversionMode, "columnConversionMode");
      return this;
    }

    /**
     * Builds a new {@link ImmutableBarrageSubscriptionOptions ImmutableBarrageSubscriptionOptions}.
     * @return An immutable instance of BarrageSubscriptionOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBarrageSubscriptionOptions build() {
      return new ImmutableBarrageSubscriptionOptions(this);
    }

    private boolean useDeephavenNullsIsSet() {
      return (optBits & OPT_BIT_USE_DEEPHAVEN_NULLS) != 0;
    }

    private boolean columnsAsListIsSet() {
      return (optBits & OPT_BIT_COLUMNS_AS_LIST) != 0;
    }

    private boolean minUpdateIntervalMsIsSet() {
      return (optBits & OPT_BIT_MIN_UPDATE_INTERVAL_MS) != 0;
    }

    private boolean batchSizeIsSet() {
      return (optBits & OPT_BIT_BATCH_SIZE) != 0;
    }

    private boolean maxMessageSizeIsSet() {
      return (optBits & OPT_BIT_MAX_MESSAGE_SIZE) != 0;
    }
  }
}
