package io.deephaven.server.netty;

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
 * Immutable implementation of {@link NettyConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNettyConfig.builder()}.
 */
@Generated(from = "NettyConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableNettyConfig extends NettyConfig {
  private final int port;

  private ImmutableNettyConfig(ImmutableNettyConfig.Builder builder) {
    this.port = builder.portIsSet()
        ? builder.port
        : super.port();
  }

  private ImmutableNettyConfig(int port) {
    this.port = port;
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
   * Copy the current immutable object by setting a value for the {@link NettyConfig#port() port} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for port
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableNettyConfig withPort(int value) {
    if (this.port == value) return this;
    return new ImmutableNettyConfig(value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNettyConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNettyConfig
        && equalTo(0, (ImmutableNettyConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableNettyConfig another) {
    return port == another.port;
  }

  /**
   * Computes a hash code from attributes: {@code port}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + port;
    return h;
  }

  /**
   * Prints the immutable value {@code NettyConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("NettyConfig")
        .omitNullValues()
        .add("port", port)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link NettyConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable NettyConfig instance
   */
  public static ImmutableNettyConfig copyOf(NettyConfig instance) {
    if (instance instanceof ImmutableNettyConfig) {
      return (ImmutableNettyConfig) instance;
    }
    return ImmutableNettyConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableNettyConfig ImmutableNettyConfig}.
   * <pre>
   * ImmutableNettyConfig.builder()
   *    .port(int) // optional {@link NettyConfig#port() port}
   *    .build();
   * </pre>
   * @return A new ImmutableNettyConfig builder
   */
  public static ImmutableNettyConfig.Builder builder() {
    return new ImmutableNettyConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNettyConfig ImmutableNettyConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NettyConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements NettyConfig.Builder {
    private static final long OPT_BIT_PORT = 0x1L;
    private long optBits;

    private int port;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code NettyConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(NettyConfig instance) {
      Objects.requireNonNull(instance, "instance");
      port(instance.port());
      return this;
    }

    /**
     * Initializes the value for the {@link NettyConfig#port() port} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link NettyConfig#port() port}.</em>
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
     * Builds a new {@link ImmutableNettyConfig ImmutableNettyConfig}.
     * @return An immutable instance of NettyConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNettyConfig build() {
      return new ImmutableNettyConfig(this);
    }

    private boolean portIsSet() {
      return (optBits & OPT_BIT_PORT) != 0;
    }
  }
}
