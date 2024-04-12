package io.deephaven.server.jetty;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
  private final int port;
  private final @Nullable JettyConfig.WebsocketsSupport websockets;
  private final @Nullable Boolean http1;
  private final boolean sniHostCheck;
  private final @Nullable Long http2StreamIdleTimeout;
  private final @Nullable Boolean httpCompression;

  private ImmutableJettyConfig(ImmutableJettyConfig.Builder builder) {
    this.websockets = builder.websockets;
    this.http1 = builder.http1;
    this.http2StreamIdleTimeout = builder.http2StreamIdleTimeout;
    this.httpCompression = builder.httpCompression;
    if (builder.portIsSet()) {
      initShim.port(builder.port);
    }
    if (builder.sniHostCheckIsSet()) {
      initShim.sniHostCheck(builder.sniHostCheck);
    }
    this.port = initShim.port();
    this.sniHostCheck = initShim.sniHostCheck();
    this.initShim = null;
  }

  private ImmutableJettyConfig(
      int port,
      @Nullable JettyConfig.WebsocketsSupport websockets,
      @Nullable Boolean http1,
      boolean sniHostCheck,
      @Nullable Long http2StreamIdleTimeout,
      @Nullable Boolean httpCompression) {
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
      if (portBuildStage == STAGE_INITIALIZING) attributes.add("port");
      if (sniHostCheckBuildStage == STAGE_INITIALIZING) attributes.add("sniHostCheck");
      return "Cannot build JettyConfig, attribute initializers form cycle " + attributes;
    }
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
   * Copy the current immutable object by setting a value for the {@link JettyConfig#port() port} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for port
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withPort(int value) {
    if (this.port == value) return this;
    return new ImmutableJettyConfig(
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
    return new ImmutableJettyConfig(this.port, this.websockets, this.http1, this.sniHostCheck, newValue, this.httpCompression);
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
    return new ImmutableJettyConfig(this.port, this.websockets, this.http1, this.sniHostCheck, value, this.httpCompression);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#httpCompression() httpCompression} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for httpCompression (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withHttpCompression(@Nullable Boolean value) {
    if (Objects.equals(this.httpCompression, value)) return this;
    return new ImmutableJettyConfig(this.port, this.websockets, this.http1, this.sniHostCheck, this.http2StreamIdleTimeout, value);
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
    return port == another.port
        && Objects.equals(websockets, another.websockets)
        && Objects.equals(http1, another.http1)
        && sniHostCheck == another.sniHostCheck
        && Objects.equals(http2StreamIdleTimeout, another.http2StreamIdleTimeout)
        && Objects.equals(httpCompression, another.httpCompression);
  }

  /**
   * Computes a hash code from attributes: {@code port}, {@code websockets}, {@code http1}, {@code sniHostCheck}, {@code http2StreamIdleTimeout}, {@code httpCompression}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
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
    private static final long OPT_BIT_PORT = 0x1L;
    private static final long OPT_BIT_SNI_HOST_CHECK = 0x2L;
    private long optBits;

    private int port;
    private @Nullable JettyConfig.WebsocketsSupport websockets;
    private @Nullable Boolean http1;
    private boolean sniHostCheck;
    private @Nullable Long http2StreamIdleTimeout;
    private @Nullable Boolean httpCompression;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code JettyConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(JettyConfig instance) {
      Objects.requireNonNull(instance, "instance");
      port(instance.port());
      @Nullable JettyConfig.WebsocketsSupport websocketsValue = instance.websockets();
      if (websocketsValue != null) {
        websockets(websocketsValue);
      }
      @Nullable Boolean http1Value = instance.http1();
      if (http1Value != null) {
        http1(http1Value);
      }
      sniHostCheck(instance.sniHostCheck());
      OptionalLong http2StreamIdleTimeoutOptional = instance.http2StreamIdleTimeout();
      if (http2StreamIdleTimeoutOptional.isPresent()) {
        http2StreamIdleTimeout(http2StreamIdleTimeoutOptional);
      }
      @Nullable Boolean httpCompressionValue = instance.httpCompression();
      if (httpCompressionValue != null) {
        httpCompression(httpCompressionValue);
      }
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
      return new ImmutableJettyConfig(this);
    }

    private boolean portIsSet() {
      return (optBits & OPT_BIT_PORT) != 0;
    }

    private boolean sniHostCheckIsSet() {
      return (optBits & OPT_BIT_SNI_HOST_CHECK) != 0;
    }
  }
}
