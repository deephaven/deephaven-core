package io.deephaven.server.runner;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.ssl.config.SSLConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DeephavenApiServerTestConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDeephavenApiServerTestConfig.builder()}.
 */
@Generated(from = "DeephavenApiServerTestConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableDeephavenApiServerTestConfig
    extends DeephavenApiServerTestConfig {
  private final @Nullable String host;
  private final int maxInboundMessageSize;
  private final @Nullable SSLConfig outboundSsl;
  private final int port;
  private final @Nullable Boolean proxyHint;
  private final int schedulerPoolSize;
  private final Duration shutdownTimeout;
  private final @Nullable SSLConfig ssl;
  private final @Nullable String targetUrl;
  private final Duration tokenExpire;

  private ImmutableDeephavenApiServerTestConfig(ImmutableDeephavenApiServerTestConfig.Builder builder) {
    this.host = builder.host;
    this.outboundSsl = builder.outboundSsl;
    this.port = builder.port;
    this.proxyHint = builder.proxyHint;
    this.ssl = builder.ssl;
    this.targetUrl = builder.targetUrl;
    this.tokenExpire = builder.tokenExpire;
    if (builder.maxInboundMessageSizeIsSet()) {
      initShim.maxInboundMessageSize(builder.maxInboundMessageSize);
    }
    if (builder.schedulerPoolSizeIsSet()) {
      initShim.schedulerPoolSize(builder.schedulerPoolSize);
    }
    if (builder.shutdownTimeout != null) {
      initShim.shutdownTimeout(builder.shutdownTimeout);
    }
    this.maxInboundMessageSize = initShim.maxInboundMessageSize();
    this.schedulerPoolSize = initShim.schedulerPoolSize();
    this.shutdownTimeout = initShim.shutdownTimeout();
    this.initShim = null;
  }

  private ImmutableDeephavenApiServerTestConfig(
      @Nullable String host,
      int maxInboundMessageSize,
      @Nullable SSLConfig outboundSsl,
      int port,
      @Nullable Boolean proxyHint,
      int schedulerPoolSize,
      Duration shutdownTimeout,
      @Nullable SSLConfig ssl,
      @Nullable String targetUrl,
      Duration tokenExpire) {
    this.host = host;
    this.maxInboundMessageSize = maxInboundMessageSize;
    this.outboundSsl = outboundSsl;
    this.port = port;
    this.proxyHint = proxyHint;
    this.schedulerPoolSize = schedulerPoolSize;
    this.shutdownTimeout = shutdownTimeout;
    this.ssl = ssl;
    this.targetUrl = targetUrl;
    this.tokenExpire = tokenExpire;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "DeephavenApiServerTestConfig", generator = "Immutables")
  private final class InitShim {
    private byte maxInboundMessageSizeBuildStage = STAGE_UNINITIALIZED;
    private int maxInboundMessageSize;

    int maxInboundMessageSize() {
      if (maxInboundMessageSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maxInboundMessageSizeBuildStage == STAGE_UNINITIALIZED) {
        maxInboundMessageSizeBuildStage = STAGE_INITIALIZING;
        this.maxInboundMessageSize = ImmutableDeephavenApiServerTestConfig.super.maxInboundMessageSize();
        maxInboundMessageSizeBuildStage = STAGE_INITIALIZED;
      }
      return this.maxInboundMessageSize;
    }

    void maxInboundMessageSize(int maxInboundMessageSize) {
      this.maxInboundMessageSize = maxInboundMessageSize;
      maxInboundMessageSizeBuildStage = STAGE_INITIALIZED;
    }

    private byte schedulerPoolSizeBuildStage = STAGE_UNINITIALIZED;
    private int schedulerPoolSize;

    int schedulerPoolSize() {
      if (schedulerPoolSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (schedulerPoolSizeBuildStage == STAGE_UNINITIALIZED) {
        schedulerPoolSizeBuildStage = STAGE_INITIALIZING;
        this.schedulerPoolSize = ImmutableDeephavenApiServerTestConfig.super.schedulerPoolSize();
        schedulerPoolSizeBuildStage = STAGE_INITIALIZED;
      }
      return this.schedulerPoolSize;
    }

    void schedulerPoolSize(int schedulerPoolSize) {
      this.schedulerPoolSize = schedulerPoolSize;
      schedulerPoolSizeBuildStage = STAGE_INITIALIZED;
    }

    private byte shutdownTimeoutBuildStage = STAGE_UNINITIALIZED;
    private Duration shutdownTimeout;

    Duration shutdownTimeout() {
      if (shutdownTimeoutBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (shutdownTimeoutBuildStage == STAGE_UNINITIALIZED) {
        shutdownTimeoutBuildStage = STAGE_INITIALIZING;
        this.shutdownTimeout = Objects.requireNonNull(ImmutableDeephavenApiServerTestConfig.super.shutdownTimeout(), "shutdownTimeout");
        shutdownTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.shutdownTimeout;
    }

    void shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = shutdownTimeout;
      shutdownTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (maxInboundMessageSizeBuildStage == STAGE_INITIALIZING) attributes.add("maxInboundMessageSize");
      if (schedulerPoolSizeBuildStage == STAGE_INITIALIZING) attributes.add("schedulerPoolSize");
      if (shutdownTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("shutdownTimeout");
      return "Cannot build DeephavenApiServerTestConfig, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code host} attribute
   */
  @Override
  public Optional<String> host() {
    return Optional.ofNullable(host);
  }

  /**
   * @return The value of the {@code maxInboundMessageSize} attribute
   */
  @Override
  public int maxInboundMessageSize() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.maxInboundMessageSize()
        : this.maxInboundMessageSize;
  }

  /**
   * @return The value of the {@code outboundSsl} attribute
   */
  @Override
  public Optional<SSLConfig> outboundSsl() {
    return Optional.ofNullable(outboundSsl);
  }

  /**
   * @return The value of the {@code port} attribute
   */
  @Override
  public int port() {
    return port;
  }

  /**
   * @return The value of the {@code proxyHint} attribute
   */
  @Override
  public @Nullable Boolean proxyHint() {
    return proxyHint;
  }

  /**
   * @return The value of the {@code schedulerPoolSize} attribute
   */
  @Override
  public int schedulerPoolSize() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.schedulerPoolSize()
        : this.schedulerPoolSize;
  }

  /**
   * @return The value of the {@code shutdownTimeout} attribute
   */
  @Override
  public Duration shutdownTimeout() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.shutdownTimeout()
        : this.shutdownTimeout;
  }

  /**
   * @return The value of the {@code ssl} attribute
   */
  @Override
  public Optional<SSLConfig> ssl() {
    return Optional.ofNullable(ssl);
  }

  /**
   * @return The value of the {@code targetUrl} attribute
   */
  @Override
  public Optional<String> targetUrl() {
    return Optional.ofNullable(targetUrl);
  }

  /**
   * @return The value of the {@code tokenExpire} attribute
   */
  @Override
  public Duration tokenExpire() {
    return tokenExpire;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DeephavenApiServerTestConfig#host() host} attribute.
   * @param value The value for host
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withHost(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "host");
    if (Objects.equals(this.host, newValue)) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        newValue,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DeephavenApiServerTestConfig#host() host} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for host
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withHost(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.host, value)) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        value,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxInboundMessageSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withMaxInboundMessageSize(int value) {
    if (this.maxInboundMessageSize == value) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        value,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DeephavenApiServerTestConfig#outboundSsl() outboundSsl} attribute.
   * @param value The value for outboundSsl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withOutboundSsl(SSLConfig value) {
    @Nullable SSLConfig newValue = Objects.requireNonNull(value, "outboundSsl");
    if (this.outboundSsl == newValue) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        newValue,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DeephavenApiServerTestConfig#outboundSsl() outboundSsl} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for outboundSsl
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableDeephavenApiServerTestConfig withOutboundSsl(Optional<? extends SSLConfig> optional) {
    @Nullable SSLConfig value = optional.orElse(null);
    if (this.outboundSsl == value) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        value,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#port() port} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for port
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withPort(int value) {
    if (this.port == value) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        value,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#proxyHint() proxyHint} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for proxyHint (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withProxyHint(@Nullable Boolean value) {
    if (Objects.equals(this.proxyHint, value)) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        value,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#schedulerPoolSize() schedulerPoolSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schedulerPoolSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withSchedulerPoolSize(int value) {
    if (this.schedulerPoolSize == value) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        value,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#shutdownTimeout() shutdownTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for shutdownTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withShutdownTimeout(Duration value) {
    if (this.shutdownTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "shutdownTimeout");
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        newValue,
        this.ssl,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DeephavenApiServerTestConfig#ssl() ssl} attribute.
   * @param value The value for ssl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withSsl(SSLConfig value) {
    @Nullable SSLConfig newValue = Objects.requireNonNull(value, "ssl");
    if (this.ssl == newValue) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        newValue,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DeephavenApiServerTestConfig#ssl() ssl} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for ssl
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableDeephavenApiServerTestConfig withSsl(Optional<? extends SSLConfig> optional) {
    @Nullable SSLConfig value = optional.orElse(null);
    if (this.ssl == value) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        value,
        this.targetUrl,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DeephavenApiServerTestConfig#targetUrl() targetUrl} attribute.
   * @param value The value for targetUrl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withTargetUrl(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "targetUrl");
    if (Objects.equals(this.targetUrl, newValue)) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        newValue,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DeephavenApiServerTestConfig#targetUrl() targetUrl} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for targetUrl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withTargetUrl(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.targetUrl, value)) return this;
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        value,
        this.tokenExpire);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenApiServerTestConfig#tokenExpire() tokenExpire} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for tokenExpire
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenApiServerTestConfig withTokenExpire(Duration value) {
    if (this.tokenExpire == value) return this;
    Duration newValue = Objects.requireNonNull(value, "tokenExpire");
    return new ImmutableDeephavenApiServerTestConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.port,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDeephavenApiServerTestConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDeephavenApiServerTestConfig
        && equalTo(0, (ImmutableDeephavenApiServerTestConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableDeephavenApiServerTestConfig another) {
    return Objects.equals(host, another.host)
        && maxInboundMessageSize == another.maxInboundMessageSize
        && Objects.equals(outboundSsl, another.outboundSsl)
        && port == another.port
        && Objects.equals(proxyHint, another.proxyHint)
        && schedulerPoolSize == another.schedulerPoolSize
        && shutdownTimeout.equals(another.shutdownTimeout)
        && Objects.equals(ssl, another.ssl)
        && Objects.equals(targetUrl, another.targetUrl)
        && tokenExpire.equals(another.tokenExpire);
  }

  /**
   * Computes a hash code from attributes: {@code host}, {@code maxInboundMessageSize}, {@code outboundSsl}, {@code port}, {@code proxyHint}, {@code schedulerPoolSize}, {@code shutdownTimeout}, {@code ssl}, {@code targetUrl}, {@code tokenExpire}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(host);
    h += (h << 5) + maxInboundMessageSize;
    h += (h << 5) + Objects.hashCode(outboundSsl);
    h += (h << 5) + port;
    h += (h << 5) + Objects.hashCode(proxyHint);
    h += (h << 5) + schedulerPoolSize;
    h += (h << 5) + shutdownTimeout.hashCode();
    h += (h << 5) + Objects.hashCode(ssl);
    h += (h << 5) + Objects.hashCode(targetUrl);
    h += (h << 5) + tokenExpire.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code DeephavenApiServerTestConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("DeephavenApiServerTestConfig")
        .omitNullValues()
        .add("host", host)
        .add("maxInboundMessageSize", maxInboundMessageSize)
        .add("outboundSsl", outboundSsl)
        .add("port", port)
        .add("proxyHint", proxyHint)
        .add("schedulerPoolSize", schedulerPoolSize)
        .add("shutdownTimeout", shutdownTimeout)
        .add("ssl", ssl)
        .add("targetUrl", targetUrl)
        .add("tokenExpire", tokenExpire)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link DeephavenApiServerTestConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DeephavenApiServerTestConfig instance
   */
  public static ImmutableDeephavenApiServerTestConfig copyOf(DeephavenApiServerTestConfig instance) {
    if (instance instanceof ImmutableDeephavenApiServerTestConfig) {
      return (ImmutableDeephavenApiServerTestConfig) instance;
    }
    return ImmutableDeephavenApiServerTestConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
   * <pre>
   * ImmutableDeephavenApiServerTestConfig.builder()
   *    .host(String) // optional {@link DeephavenApiServerTestConfig#host() host}
   *    .maxInboundMessageSize(int) // optional {@link DeephavenApiServerTestConfig#maxInboundMessageSize() maxInboundMessageSize}
   *    .outboundSsl(io.deephaven.ssl.config.SSLConfig) // optional {@link DeephavenApiServerTestConfig#outboundSsl() outboundSsl}
   *    .port(int) // required {@link DeephavenApiServerTestConfig#port() port}
   *    .proxyHint(Boolean | null) // nullable {@link DeephavenApiServerTestConfig#proxyHint() proxyHint}
   *    .schedulerPoolSize(int) // optional {@link DeephavenApiServerTestConfig#schedulerPoolSize() schedulerPoolSize}
   *    .shutdownTimeout(java.time.Duration) // optional {@link DeephavenApiServerTestConfig#shutdownTimeout() shutdownTimeout}
   *    .ssl(io.deephaven.ssl.config.SSLConfig) // optional {@link DeephavenApiServerTestConfig#ssl() ssl}
   *    .targetUrl(String) // optional {@link DeephavenApiServerTestConfig#targetUrl() targetUrl}
   *    .tokenExpire(java.time.Duration) // required {@link DeephavenApiServerTestConfig#tokenExpire() tokenExpire}
   *    .build();
   * </pre>
   * @return A new ImmutableDeephavenApiServerTestConfig builder
   */
  public static ImmutableDeephavenApiServerTestConfig.Builder builder() {
    return new ImmutableDeephavenApiServerTestConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DeephavenApiServerTestConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements DeephavenApiServerTestConfig.Builder {
    private static final long INIT_BIT_PORT = 0x1L;
    private static final long INIT_BIT_TOKEN_EXPIRE = 0x2L;
    private static final long OPT_BIT_MAX_INBOUND_MESSAGE_SIZE = 0x1L;
    private static final long OPT_BIT_SCHEDULER_POOL_SIZE = 0x2L;
    private long initBits = 0x3L;
    private long optBits;

    private @Nullable String host;
    private int maxInboundMessageSize;
    private @Nullable SSLConfig outboundSsl;
    private int port;
    private @Nullable Boolean proxyHint;
    private int schedulerPoolSize;
    private @Nullable Duration shutdownTimeout;
    private @Nullable SSLConfig ssl;
    private @Nullable String targetUrl;
    private @Nullable Duration tokenExpire;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.server.config.ServerConfig} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ServerConfig instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.server.runner.DeephavenApiServerTestConfig} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(DeephavenApiServerTestConfig instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      if (object instanceof ServerConfig) {
        ServerConfig instance = (ServerConfig) object;
        port(instance.port());
        schedulerPoolSize(instance.schedulerPoolSize());
        Optional<String> hostOptional = instance.host();
        if (hostOptional.isPresent()) {
          host(hostOptional);
        }
        shutdownTimeout(instance.shutdownTimeout());
        Optional<SSLConfig> outboundSslOptional = instance.outboundSsl();
        if (outboundSslOptional.isPresent()) {
          outboundSsl(outboundSslOptional);
        }
        maxInboundMessageSize(instance.maxInboundMessageSize());
        Optional<SSLConfig> sslOptional = instance.ssl();
        if (sslOptional.isPresent()) {
          ssl(sslOptional);
        }
        Optional<String> targetUrlOptional = instance.targetUrl();
        if (targetUrlOptional.isPresent()) {
          targetUrl(targetUrlOptional);
        }
        tokenExpire(instance.tokenExpire());
        @Nullable Boolean proxyHintValue = instance.proxyHint();
        if (proxyHintValue != null) {
          proxyHint(proxyHintValue);
        }
      }
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#host() host} to host.
     * @param host The value for host
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder host(String host) {
      this.host = Objects.requireNonNull(host, "host");
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#host() host} to host.
     * @param host The value for host
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder host(Optional<String> host) {
      this.host = host.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DeephavenApiServerTestConfig#maxInboundMessageSize() maxInboundMessageSize}.</em>
     * @param maxInboundMessageSize The value for maxInboundMessageSize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder maxInboundMessageSize(int maxInboundMessageSize) {
      this.maxInboundMessageSize = maxInboundMessageSize;
      optBits |= OPT_BIT_MAX_INBOUND_MESSAGE_SIZE;
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#outboundSsl() outboundSsl} to outboundSsl.
     * @param outboundSsl The value for outboundSsl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder outboundSsl(SSLConfig outboundSsl) {
      this.outboundSsl = Objects.requireNonNull(outboundSsl, "outboundSsl");
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#outboundSsl() outboundSsl} to outboundSsl.
     * @param outboundSsl The value for outboundSsl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder outboundSsl(Optional<? extends SSLConfig> outboundSsl) {
      this.outboundSsl = outboundSsl.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#port() port} attribute.
     * @param port The value for port 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder port(int port) {
      this.port = port;
      initBits &= ~INIT_BIT_PORT;
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#proxyHint() proxyHint} attribute.
     * @param proxyHint The value for proxyHint (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder proxyHint(@Nullable Boolean proxyHint) {
      this.proxyHint = proxyHint;
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#schedulerPoolSize() schedulerPoolSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DeephavenApiServerTestConfig#schedulerPoolSize() schedulerPoolSize}.</em>
     * @param schedulerPoolSize The value for schedulerPoolSize 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder schedulerPoolSize(int schedulerPoolSize) {
      this.schedulerPoolSize = schedulerPoolSize;
      optBits |= OPT_BIT_SCHEDULER_POOL_SIZE;
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#shutdownTimeout() shutdownTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link DeephavenApiServerTestConfig#shutdownTimeout() shutdownTimeout}.</em>
     * @param shutdownTimeout The value for shutdownTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(SSLConfig ssl) {
      this.ssl = Objects.requireNonNull(ssl, "ssl");
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(Optional<? extends SSLConfig> ssl) {
      this.ssl = ssl.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#targetUrl() targetUrl} to targetUrl.
     * @param targetUrl The value for targetUrl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder targetUrl(String targetUrl) {
      this.targetUrl = Objects.requireNonNull(targetUrl, "targetUrl");
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenApiServerTestConfig#targetUrl() targetUrl} to targetUrl.
     * @param targetUrl The value for targetUrl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder targetUrl(Optional<String> targetUrl) {
      this.targetUrl = targetUrl.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenApiServerTestConfig#tokenExpire() tokenExpire} attribute.
     * @param tokenExpire The value for tokenExpire 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tokenExpire(Duration tokenExpire) {
      this.tokenExpire = Objects.requireNonNull(tokenExpire, "tokenExpire");
      initBits &= ~INIT_BIT_TOKEN_EXPIRE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDeephavenApiServerTestConfig ImmutableDeephavenApiServerTestConfig}.
     * @return An immutable instance of DeephavenApiServerTestConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDeephavenApiServerTestConfig build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableDeephavenApiServerTestConfig(this);
    }

    private boolean maxInboundMessageSizeIsSet() {
      return (optBits & OPT_BIT_MAX_INBOUND_MESSAGE_SIZE) != 0;
    }

    private boolean schedulerPoolSizeIsSet() {
      return (optBits & OPT_BIT_SCHEDULER_POOL_SIZE) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PORT) != 0) attributes.add("port");
      if ((initBits & INIT_BIT_TOKEN_EXPIRE) != 0) attributes.add("tokenExpire");
      return "Cannot build DeephavenApiServerTestConfig, some of required attributes are not set " + attributes;
    }
  }
}
