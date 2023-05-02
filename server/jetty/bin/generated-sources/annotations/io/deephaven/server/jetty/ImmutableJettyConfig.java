package io.deephaven.server.jetty;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
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

  private ImmutableJettyConfig(ImmutableJettyConfig.Builder builder) {
    this.websockets = builder.websockets;
    this.http1 = builder.http1;
    this.port = builder.portIsSet()
        ? builder.port
        : super.port();
  }

  private ImmutableJettyConfig(
      int port,
      @Nullable JettyConfig.WebsocketsSupport websockets,
      @Nullable Boolean http1) {
    this.port = port;
    this.websockets = websockets;
    this.http1 = http1;
  }

  /**
   * The port. Defaults to {@value DEFAULT_SSL_PORT} if {@link #ssl()} is present, otherwise defaults to
   * {@value DEFAULT_PLAINTEXT_PORT}.
   */
  @Override
  public int port() {
    return port;
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
   * Copy the current immutable object by setting a value for the {@link JettyConfig#port() port} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for port
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withPort(int value) {
    if (this.port == value) return this;
    return new ImmutableJettyConfig(value, this.websockets, this.http1);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#websockets() websockets} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for websockets (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withWebsockets(@Nullable JettyConfig.WebsocketsSupport value) {
    if (this.websockets == value) return this;
    return new ImmutableJettyConfig(this.port, value, this.http1);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link JettyConfig#http1() http1} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for http1 (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableJettyConfig withHttp1(@Nullable Boolean value) {
    if (Objects.equals(this.http1, value)) return this;
    return new ImmutableJettyConfig(this.port, this.websockets, value);
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
        && Objects.equals(http1, another.http1);
  }

  /**
   * Computes a hash code from attributes: {@code port}, {@code websockets}, {@code http1}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + port;
    h += (h << 5) + Objects.hashCode(websockets);
    h += (h << 5) + Objects.hashCode(http1);
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
    private long optBits;

    private int port;
    private @Nullable JettyConfig.WebsocketsSupport websockets;
    private @Nullable Boolean http1;

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
  }
}
