package io.deephaven.server.jetty;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.ssl.config.SSLConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link JettyConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableJettyConfig.builder()}.
 */
@Generated(from = "JettyConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableJettyConfig extends JettyConfig {
  private final @Nullable String host;
  private final int maxInboundMessageSize;
  private final @Nullable SSLConfig outboundSsl;
  private final @Nullable Boolean proxyHint;
  private final int schedulerPoolSize;
  private final Duration shutdownTimeout;
  private final @Nullable SSLConfig ssl;
  private final @Nullable String targetUrl;
  private final Duration tokenExpire;
  private final int port;
  private final @Nullable JettyConfig.WebsocketsSupport websockets;
  private final @Nullable Boolean http1;
  private final boolean sniHostCheck;
  private final @Nullable Long http2StreamIdleTimeout;
  private final @Nullable Boolean httpCompression;

  private ImmutableJettyConfig(ImmutableJettyConfig.Builder builder) {
    this.host = builder.host;
    this.outboundSsl = builder.outboundSsl;
    this.proxyHint = builder.proxyHint;
    this.ssl = builder.ssl;
    this.targetUrl = builder.targetUrl;
    this.tokenExpire = builder.tokenExpire;
    this.websockets = builder.websockets;
    this.http1 = builder.http1;
    this.http2StreamIdleTimeout = builder.http2StreamIdleTimeout;
    this.httpCompression = builder.httpCompression;
    if (builder.maxInboundMessageSizeIsSet()) {
      initShim.maxInboundMessageSize(builder.maxInboundMessageSize);
    }
    if (builder.schedulerPoolSizeIsSet()) {
      initShim.schedulerPoolSize(builder.schedulerPoolSize);
    }
    if (builder.shutdownTimeout != null) {
      initShim.shutdownTimeout(builder.shutdownTimeout);
    }
    if (builder.portIsSet()) {
      initShim.port(builder.port);
    }
    if (builder.sniHostCheckIsSet()) {
      initShim.sniHostCheck(builder.sniHostCheck);
    }
    this.maxInboundMessageSize = initShim.maxInboundMessageSize();
    this.schedulerPoolSize = initShim.schedulerPoolSize();
    this.shutdownTimeout = initShim.shutdownTimeout();
    this.port = initShim.port();
    this.sniHostCheck = initShim.sniHostCheck();
    this.initShim = null;
  }

  private ImmutableJettyConfig(
      @Nullable String host,
      int maxInboundMessageSize,
      @Nullable SSLConfig outboundSsl,
      @Nullable Boolean proxyHint,
      int schedulerPoolSize,
      Duration shutdownTimeout,
      @Nullable SSLConfig ssl,
      @Nullable String targetUrl,
      Duration tokenExpire,
      int port,
      @Nullable JettyConfig.WebsocketsSupport websockets,
      @Nullable Boolean http1,
      boolean sniHostCheck,
      @Nullable Long http2StreamIdleTimeout,
      @Nullable Boolean httpCompression) {
    this.host = host;
    this.maxInboundMessageSize = maxInboundMessageSize;
    this.outboundSsl = outboundSsl;
    this.proxyHint = proxyHint;
    this.schedulerPoolSize = schedulerPoolSize;
    this.shutdownTimeout = shutdownTimeout;
    this.ssl = ssl;
    this.targetUrl = targetUrl;
    this.tokenExpire = tokenExpire;
    this.port = port;
    this.websockets = websockets;
    this.http1 = http1;
    this.sniHostCheck = sniHostCheck;
    this.http2StreamIdleTimeout = http2StreamIdleTimeout;
    this.httpCompression = httpCompression;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "JettyConfig", generator = "Immutables")
  private final class InitShim {
    private byte maxInboundMessageSizeBuildStage = STAGE_UNINITIALIZED;
    private int maxInboundMessageSize;

    int maxInboundMessageSize() {
      if (maxInboundMessageSizeBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (maxInboundMessageSizeBuildStage == STAGE_UNINITIALIZED) {
        maxInboundMessageSizeBuildStage = STAGE_INITIALIZING;
        this.maxInboundMessageSize = ImmutableJettyConfig.super.maxInboundMessageSize();
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
        this.schedulerPoolSize = ImmutableJettyConfig.super.schedulerPoolSize();
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
        this.shutdownTimeout = Objects.requireNonNull(ImmutableJettyConfig.super.shutdownTimeout(), "shutdownTimeout");
        shutdownTimeoutBuildStage = STAGE_INITIALIZED;
      }
      return this.shutdownTimeout;
    }

    void shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = shutdownTimeout;
      shutdownTimeoutBuildStage = STAGE_INITIALIZED;
    }

    private byte portBuildStage = STAGE_UNINITIALIZED;
    private int port;

    int port() {
      if (portBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (portBuildStage == STAGE_UNINITIALIZED) {
        portBuildStage = STAGE_INITIALIZING;
        this.port = ImmutableJettyConfig.super.port();
        portBuildStage = STAGE_INITIALIZED;
      }
      return this.port;
    }

    void port(int port) {
      this.port = port;
      portBuildStage = STAGE_INITIALIZED;
    }

    private byte sniHostCheckBuildStage = STAGE_UNINITIALIZED;
    private boolean sniHostCheck;

    boolean sniHostCheck() {
      if (sniHostCheckBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (sniHostCheckBuildStage == STAGE_UNINITIALIZED) {
        sniHostCheckBuildStage = STAGE_INITIALIZING;
        this.sniHostCheck = ImmutableJettyConfig.super.sniHostCheck();
        sniHostCheckBuildStage = STAGE_INITIALIZED;
      }
      return this.sniHostCheck;
    }

    void sniHostCheck(boolean sniHostCheck) {
      this.sniHostCheck = sniHostCheck;
      sniHostCheckBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (maxInboundMessageSizeBuildStage == STAGE_INITIALIZING) attributes.add("maxInboundMessageSize");
      if (schedulerPoolSizeBuildStage == STAGE_INITIALIZING) attributes.add("schedulerPoolSize");
      if (shutdownTimeoutBuildStage == STAGE_INITIALIZING) attributes.add("shutdownTimeout");
      if (portBuildStage == STAGE_INITIALIZING) attributes.add("port");
      if (sniHostCheckBuildStage == STAGE_INITIALIZING) attributes.add("sniHostCheck");
      return "Cannot build JettyConfig, attribute initializers form cycle " + attributes;
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
   * The port. Defaults to {@value DEFAULT_SSL_PORT} if {@link #ssl()} is present, otherwise defaults to
   * {@value DEFAULT_PLAINTEXT_PORT}.
   */
  @Override
  public int port() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.port()
        : this.port;
  }

  /**
   * Include websockets.
   */
  @Override
  public @Nullable JettyConfig.WebsocketsSupport websockets() {
    return websockets;
  }

  /**
   * Include HTTP/1.1.
   */
  @Override
  public @Nullable Boolean http1() {
    return http1;
  }

  /**
   * Include sniHostCheck.
   */
  @Override
  public boolean sniHostCheck() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.sniHostCheck()
        : this.sniHostCheck;
  }

  /**
   * @return The value of the {@code http2StreamIdleTimeout} attribute
   */
  @Override
  public OptionalLong http2StreamIdleTimeout() {
    return http2StreamIdleTimeout != null
        ? OptionalLong.of(http2StreamIdleTimeout)
        : OptionalLong.empty();
  }

  /**
   * Include HTTP compression.
   */
  @Override
  public @Nullable Boolean httpCompression() {
    return httpCompression;
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JettyConfig#host() host} attribute.
   * @param value The value for host
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withHost(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "host");
    if (Objects.equals(this.host, newValue)) return this;
    return new ImmutableJettyConfig(
        newValue,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JettyConfig#host() host} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for host
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withHost(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.host, value)) return this;
    return new ImmutableJettyConfig(
        value,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxInboundMessageSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withMaxInboundMessageSize(int value) {
    if (this.maxInboundMessageSize == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        value,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JettyConfig#outboundSsl() outboundSsl} attribute.
   * @param value The value for outboundSsl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withOutboundSsl(SSLConfig value) {
    @Nullable SSLConfig newValue = Objects.requireNonNull(value, "outboundSsl");
    if (this.outboundSsl == newValue) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        newValue,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JettyConfig#outboundSsl() outboundSsl} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for outboundSsl
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableJettyConfig withOutboundSsl(Optional<? extends SSLConfig> optional) {
    @Nullable SSLConfig value = optional.orElse(null);
    if (this.outboundSsl == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        value,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#proxyHint() proxyHint} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for proxyHint (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withProxyHint(@Nullable Boolean value) {
    if (Objects.equals(this.proxyHint, value)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        value,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#schedulerPoolSize() schedulerPoolSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schedulerPoolSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withSchedulerPoolSize(int value) {
    if (this.schedulerPoolSize == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        value,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#shutdownTimeout() shutdownTimeout} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for shutdownTimeout
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withShutdownTimeout(Duration value) {
    if (this.shutdownTimeout == value) return this;
    Duration newValue = Objects.requireNonNull(value, "shutdownTimeout");
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        newValue,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JettyConfig#ssl() ssl} attribute.
   * @param value The value for ssl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withSsl(SSLConfig value) {
    @Nullable SSLConfig newValue = Objects.requireNonNull(value, "ssl");
    if (this.ssl == newValue) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        newValue,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JettyConfig#ssl() ssl} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for ssl
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableJettyConfig withSsl(Optional<? extends SSLConfig> optional) {
    @Nullable SSLConfig value = optional.orElse(null);
    if (this.ssl == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        value,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JettyConfig#targetUrl() targetUrl} attribute.
   * @param value The value for targetUrl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withTargetUrl(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "targetUrl");
    if (Objects.equals(this.targetUrl, newValue)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        newValue,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JettyConfig#targetUrl() targetUrl} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for targetUrl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withTargetUrl(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.targetUrl, value)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        value,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#tokenExpire() tokenExpire} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for tokenExpire
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withTokenExpire(Duration value) {
    if (this.tokenExpire == value) return this;
    Duration newValue = Objects.requireNonNull(value, "tokenExpire");
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        newValue,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#port() port} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for port
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withPort(int value) {
    if (this.port == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        value,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#websockets() websockets} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for websockets (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withWebsockets(@Nullable JettyConfig.WebsocketsSupport value) {
    if (this.websockets == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        value,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#http1() http1} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for http1 (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withHttp1(@Nullable Boolean value) {
    if (Objects.equals(this.http1, value)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        value,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#sniHostCheck() sniHostCheck} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sniHostCheck
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withSniHostCheck(boolean value) {
    if (this.sniHostCheck == value) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        value,
        this.http2StreamIdleTimeout,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link JettyConfig#http2StreamIdleTimeout() http2StreamIdleTimeout} attribute.
   * @param value The value for http2StreamIdleTimeout
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withHttp2StreamIdleTimeout(long value) {
    @Nullable Long newValue = value;
    if (Objects.equals(this.http2StreamIdleTimeout, newValue)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        newValue,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link JettyConfig#http2StreamIdleTimeout() http2StreamIdleTimeout} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for http2StreamIdleTimeout
   * @return A modified copy of {@code this} object
   */
  public final ImmutableJettyConfig withHttp2StreamIdleTimeout(OptionalLong optional) {
    @Nullable Long value = optional.isPresent() ? optional.getAsLong() : null;
    if (Objects.equals(this.http2StreamIdleTimeout, value)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        value,
        this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#httpCompression() httpCompression} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for httpCompression (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withHttpCompression(@Nullable Boolean value) {
    if (Objects.equals(this.httpCompression, value)) return this;
    return new ImmutableJettyConfig(
        this.host,
        this.maxInboundMessageSize,
        this.outboundSsl,
        this.proxyHint,
        this.schedulerPoolSize,
        this.shutdownTimeout,
        this.ssl,
        this.targetUrl,
        this.tokenExpire,
        this.port,
        this.websockets,
        this.http1,
        this.sniHostCheck,
        this.http2StreamIdleTimeout,
        value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableJettyConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableJettyConfig
        && equalTo(0, (ImmutableJettyConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableJettyConfig another) {
    return Objects.equals(host, another.host)
        && maxInboundMessageSize == another.maxInboundMessageSize
        && Objects.equals(outboundSsl, another.outboundSsl)
        && Objects.equals(proxyHint, another.proxyHint)
        && schedulerPoolSize == another.schedulerPoolSize
        && shutdownTimeout.equals(another.shutdownTimeout)
        && Objects.equals(ssl, another.ssl)
        && Objects.equals(targetUrl, another.targetUrl)
        && tokenExpire.equals(another.tokenExpire)
        && port == another.port
        && Objects.equals(websockets, another.websockets)
        && Objects.equals(http1, another.http1)
        && sniHostCheck == another.sniHostCheck
        && Objects.equals(http2StreamIdleTimeout, another.http2StreamIdleTimeout)
        && Objects.equals(httpCompression, another.httpCompression);
  }

  /**
   * Computes a hash code from attributes: {@code host}, {@code maxInboundMessageSize}, {@code outboundSsl}, {@code proxyHint}, {@code schedulerPoolSize}, {@code shutdownTimeout}, {@code ssl}, {@code targetUrl}, {@code tokenExpire}, {@code port}, {@code websockets}, {@code http1}, {@code sniHostCheck}, {@code http2StreamIdleTimeout}, {@code httpCompression}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(host);
    h += (h << 5) + maxInboundMessageSize;
    h += (h << 5) + Objects.hashCode(outboundSsl);
    h += (h << 5) + Objects.hashCode(proxyHint);
    h += (h << 5) + schedulerPoolSize;
    h += (h << 5) + shutdownTimeout.hashCode();
    h += (h << 5) + Objects.hashCode(ssl);
    h += (h << 5) + Objects.hashCode(targetUrl);
    h += (h << 5) + tokenExpire.hashCode();
    h += (h << 5) + port;
    h += (h << 5) + Objects.hashCode(websockets);
    h += (h << 5) + Objects.hashCode(http1);
    h += (h << 5) + Booleans.hashCode(sniHostCheck);
    h += (h << 5) + Objects.hashCode(http2StreamIdleTimeout);
    h += (h << 5) + Objects.hashCode(httpCompression);
    return h;
  }

  /**
   * Prints the immutable value {@code JettyConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("JettyConfig")
        .omitNullValues()
        .add("host", host)
        .add("maxInboundMessageSize", maxInboundMessageSize)
        .add("outboundSsl", outboundSsl)
        .add("proxyHint", proxyHint)
        .add("schedulerPoolSize", schedulerPoolSize)
        .add("shutdownTimeout", shutdownTimeout)
        .add("ssl", ssl)
        .add("targetUrl", targetUrl)
        .add("tokenExpire", tokenExpire)
        .add("port", port)
        .add("websockets", websockets)
        .add("http1", http1)
        .add("sniHostCheck", sniHostCheck)
        .add("http2StreamIdleTimeout", http2StreamIdleTimeout)
        .add("httpCompression", httpCompression)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link JettyConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable JettyConfig instance
   */
  public static ImmutableJettyConfig copyOf(JettyConfig instance) {
    if (instance instanceof ImmutableJettyConfig) {
      return (ImmutableJettyConfig) instance;
    }
    return ImmutableJettyConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableJettyConfig ImmutableJettyConfig}.
   * <pre>
   * ImmutableJettyConfig.builder()
   *    .host(String) // optional {@link JettyConfig#host() host}
   *    .maxInboundMessageSize(int) // optional {@link JettyConfig#maxInboundMessageSize() maxInboundMessageSize}
   *    .outboundSsl(io.deephaven.ssl.config.SSLConfig) // optional {@link JettyConfig#outboundSsl() outboundSsl}
   *    .proxyHint(Boolean | null) // nullable {@link JettyConfig#proxyHint() proxyHint}
   *    .schedulerPoolSize(int) // optional {@link JettyConfig#schedulerPoolSize() schedulerPoolSize}
   *    .shutdownTimeout(java.time.Duration) // optional {@link JettyConfig#shutdownTimeout() shutdownTimeout}
   *    .ssl(io.deephaven.ssl.config.SSLConfig) // optional {@link JettyConfig#ssl() ssl}
   *    .targetUrl(String) // optional {@link JettyConfig#targetUrl() targetUrl}
   *    .tokenExpire(java.time.Duration) // required {@link JettyConfig#tokenExpire() tokenExpire}
   *    .port(int) // optional {@link JettyConfig#port() port}
   *    .websockets(io.deephaven.server.jetty.JettyConfig.WebsocketsSupport | null) // nullable {@link JettyConfig#websockets() websockets}
   *    .http1(Boolean | null) // nullable {@link JettyConfig#http1() http1}
   *    .sniHostCheck(boolean) // optional {@link JettyConfig#sniHostCheck() sniHostCheck}
   *    .http2StreamIdleTimeout(long) // optional {@link JettyConfig#http2StreamIdleTimeout() http2StreamIdleTimeout}
   *    .httpCompression(Boolean | null) // nullable {@link JettyConfig#httpCompression() httpCompression}
   *    .build();
   * </pre>
   * @return A new ImmutableJettyConfig builder
   */
  public static ImmutableJettyConfig.Builder builder() {
    return new ImmutableJettyConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableJettyConfig ImmutableJettyConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "JettyConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements JettyConfig.Builder {
    private static final long INIT_BIT_TOKEN_EXPIRE = 0x1L;
    private static final long OPT_BIT_MAX_INBOUND_MESSAGE_SIZE = 0x1L;
    private static final long OPT_BIT_SCHEDULER_POOL_SIZE = 0x2L;
    private static final long OPT_BIT_PORT = 0x4L;
    private static final long OPT_BIT_SNI_HOST_CHECK = 0x8L;
    private long initBits = 0x1L;
    private long optBits;

    private @Nullable String host;
    private int maxInboundMessageSize;
    private @Nullable SSLConfig outboundSsl;
    private @Nullable Boolean proxyHint;
    private int schedulerPoolSize;
    private @Nullable Duration shutdownTimeout;
    private @Nullable SSLConfig ssl;
    private @Nullable String targetUrl;
    private @Nullable Duration tokenExpire;
    private int port;
    private @Nullable JettyConfig.WebsocketsSupport websockets;
    private @Nullable Boolean http1;
    private boolean sniHostCheck;
    private @Nullable Long http2StreamIdleTimeout;
    private @Nullable Boolean httpCompression;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.server.jetty.JettyConfig} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(JettyConfig instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
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

    private void from(Object object) {
      if (object instanceof JettyConfig) {
        JettyConfig instance = (JettyConfig) object;
        sniHostCheck(instance.sniHostCheck());
        @Nullable Boolean http1Value = instance.http1();
        if (http1Value != null) {
          http1(http1Value);
        }
        @Nullable Boolean httpCompressionValue = instance.httpCompression();
        if (httpCompressionValue != null) {
          httpCompression(httpCompressionValue);
        }
        OptionalLong http2StreamIdleTimeoutOptional = instance.http2StreamIdleTimeout();
        if (http2StreamIdleTimeoutOptional.isPresent()) {
          http2StreamIdleTimeout(http2StreamIdleTimeoutOptional);
        }
        @Nullable JettyConfig.WebsocketsSupport websocketsValue = instance.websockets();
        if (websocketsValue != null) {
          websockets(websocketsValue);
        }
      }
      if (object instanceof ServerConfig) {
        ServerConfig instance = (ServerConfig) object;
        schedulerPoolSize(instance.schedulerPoolSize());
        port(instance.port());
        Optional<String> hostOptional = instance.host();
        if (hostOptional.isPresent()) {
          host(hostOptional);
        }
        shutdownTimeout(instance.shutdownTimeout());
        Optional<SSLConfig> outboundSslOptional = instance.outboundSsl();
        if (outboundSslOptional.isPresent()) {
          outboundSsl(outboundSslOptional);
        }
        Optional<SSLConfig> sslOptional = instance.ssl();
        if (sslOptional.isPresent()) {
          ssl(sslOptional);
        }
        maxInboundMessageSize(instance.maxInboundMessageSize());
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
     * Initializes the optional value {@link JettyConfig#host() host} to host.
     * @param host The value for host
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder host(String host) {
      this.host = Objects.requireNonNull(host, "host");
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#host() host} to host.
     * @param host The value for host
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder host(Optional<String> host) {
      this.host = host.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JettyConfig#maxInboundMessageSize() maxInboundMessageSize}.</em>
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
     * Initializes the optional value {@link JettyConfig#outboundSsl() outboundSsl} to outboundSsl.
     * @param outboundSsl The value for outboundSsl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder outboundSsl(SSLConfig outboundSsl) {
      this.outboundSsl = Objects.requireNonNull(outboundSsl, "outboundSsl");
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#outboundSsl() outboundSsl} to outboundSsl.
     * @param outboundSsl The value for outboundSsl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder outboundSsl(Optional<? extends SSLConfig> outboundSsl) {
      this.outboundSsl = outboundSsl.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#proxyHint() proxyHint} attribute.
     * @param proxyHint The value for proxyHint (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder proxyHint(@Nullable Boolean proxyHint) {
      this.proxyHint = proxyHint;
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#schedulerPoolSize() schedulerPoolSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JettyConfig#schedulerPoolSize() schedulerPoolSize}.</em>
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
     * Initializes the value for the {@link JettyConfig#shutdownTimeout() shutdownTimeout} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JettyConfig#shutdownTimeout() shutdownTimeout}.</em>
     * @param shutdownTimeout The value for shutdownTimeout 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder shutdownTimeout(Duration shutdownTimeout) {
      this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(SSLConfig ssl) {
      this.ssl = Objects.requireNonNull(ssl, "ssl");
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(Optional<? extends SSLConfig> ssl) {
      this.ssl = ssl.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#targetUrl() targetUrl} to targetUrl.
     * @param targetUrl The value for targetUrl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder targetUrl(String targetUrl) {
      this.targetUrl = Objects.requireNonNull(targetUrl, "targetUrl");
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#targetUrl() targetUrl} to targetUrl.
     * @param targetUrl The value for targetUrl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder targetUrl(Optional<String> targetUrl) {
      this.targetUrl = targetUrl.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#tokenExpire() tokenExpire} attribute.
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
     * Initializes the value for the {@link JettyConfig#port() port} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JettyConfig#port() port}.</em>
     * @param port The value for port 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder port(int port) {
      this.port = port;
      optBits |= OPT_BIT_PORT;
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#websockets() websockets} attribute.
     * @param websockets The value for websockets (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder websockets(@Nullable JettyConfig.WebsocketsSupport websockets) {
      this.websockets = websockets;
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#http1() http1} attribute.
     * @param http1 The value for http1 (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder http1(@Nullable Boolean http1) {
      this.http1 = http1;
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#sniHostCheck() sniHostCheck} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link JettyConfig#sniHostCheck() sniHostCheck}.</em>
     * @param sniHostCheck The value for sniHostCheck 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sniHostCheck(boolean sniHostCheck) {
      this.sniHostCheck = sniHostCheck;
      optBits |= OPT_BIT_SNI_HOST_CHECK;
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#http2StreamIdleTimeout() http2StreamIdleTimeout} to http2StreamIdleTimeout.
     * @param http2StreamIdleTimeout The value for http2StreamIdleTimeout
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder http2StreamIdleTimeout(long http2StreamIdleTimeout) {
      this.http2StreamIdleTimeout = http2StreamIdleTimeout;
      return this;
    }

    /**
     * Initializes the optional value {@link JettyConfig#http2StreamIdleTimeout() http2StreamIdleTimeout} to http2StreamIdleTimeout.
     * @param http2StreamIdleTimeout The value for http2StreamIdleTimeout
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder http2StreamIdleTimeout(OptionalLong http2StreamIdleTimeout) {
      this.http2StreamIdleTimeout = http2StreamIdleTimeout.isPresent() ? http2StreamIdleTimeout.getAsLong() : null;
      return this;
    }

    /**
     * Initializes the value for the {@link JettyConfig#httpCompression() httpCompression} attribute.
     * @param httpCompression The value for httpCompression (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder httpCompression(@Nullable Boolean httpCompression) {
      this.httpCompression = httpCompression;
      return this;
    }

    /**
     * Builds a new {@link ImmutableJettyConfig ImmutableJettyConfig}.
     * @return An immutable instance of JettyConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableJettyConfig build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableJettyConfig(this);
    }

    private boolean maxInboundMessageSizeIsSet() {
      return (optBits & OPT_BIT_MAX_INBOUND_MESSAGE_SIZE) != 0;
    }

    private boolean schedulerPoolSizeIsSet() {
      return (optBits & OPT_BIT_SCHEDULER_POOL_SIZE) != 0;
    }

    private boolean portIsSet() {
      return (optBits & OPT_BIT_PORT) != 0;
    }

    private boolean sniHostCheckIsSet() {
      return (optBits & OPT_BIT_SNI_HOST_CHECK) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TOKEN_EXPIRE) != 0) attributes.add("tokenExpire");
      return "Cannot build JettyConfig, some of required attributes are not set " + attributes;
    }
  }
}
