package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
<<<<<<< HEAD
import io.deephaven.ssl.config.SSLConfig;
=======
>>>>>>> main
import io.deephaven.uri.DeephavenTarget;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ClientConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableClientConfig.builder()}.
 */
@Generated(from = "ClientConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableClientConfig extends ClientConfig {
  private final DeephavenTarget target;
  private final @Nullable SSLConfig ssl;
  private final @Nullable String userAgent;
  private final @Nullable String overrideAuthority;
  private final ImmutableMap<String, String> extraHeaders;
  private final int maxInboundMessageSize;

  private ImmutableClientConfig(ImmutableClientConfig.Builder builder) {
    this.target = builder.target;
    this.ssl = builder.ssl;
    this.userAgent = builder.userAgent;
    this.overrideAuthority = builder.overrideAuthority;
    this.extraHeaders = builder.extraHeaders.build();
    this.maxInboundMessageSize = builder.maxInboundMessageSizeIsSet()
        ? builder.maxInboundMessageSize
        : super.maxInboundMessageSize();
  }

  private ImmutableClientConfig(
      DeephavenTarget target,
      @Nullable SSLConfig ssl,
      @Nullable String userAgent,
      @Nullable String overrideAuthority,
      ImmutableMap<String, String> extraHeaders,
      int maxInboundMessageSize) {
    this.target = target;
    this.ssl = ssl;
    this.userAgent = userAgent;
    this.overrideAuthority = overrideAuthority;
    this.extraHeaders = extraHeaders;
    this.maxInboundMessageSize = maxInboundMessageSize;
  }

  /**
   * The target.
   */
  @Override
  public DeephavenTarget target() {
    return target;
  }

  /**
   * The SSL configuration. Only relevant if {@link #target()} is secure.
   */
  @Override
  public Optional<SSLConfig> ssl() {
    return Optional.ofNullable(ssl);
  }

  /**
   * The user agent.
   */
  @Override
  public Optional<String> userAgent() {
    return Optional.ofNullable(userAgent);
  }

  /**
   * The overridden authority.
   */
  @Override
  public Optional<String> overrideAuthority() {
    return Optional.ofNullable(overrideAuthority);
  }

  /**
   * The extra headers.
   */
  @Override
  public ImmutableMap<String, String> extraHeaders() {
    return extraHeaders;
  }

  /**
   * The maximum inbound message size. Defaults to 100MiB.
   */
  @Override
  public int maxInboundMessageSize() {
    return maxInboundMessageSize;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ClientConfig#target() target} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for target
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableClientConfig withTarget(DeephavenTarget value) {
    if (this.target == value) return this;
    DeephavenTarget newValue = Objects.requireNonNull(value, "target");
    return new ImmutableClientConfig(
        newValue,
        this.ssl,
        this.userAgent,
        this.overrideAuthority,
        this.extraHeaders,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ClientConfig#ssl() ssl} attribute.
   * @param value The value for ssl
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withSsl(SSLConfig value) {
    @Nullable SSLConfig newValue = Objects.requireNonNull(value, "ssl");
    if (this.ssl == newValue) return this;
    return new ImmutableClientConfig(
        this.target,
        newValue,
        this.userAgent,
        this.overrideAuthority,
        this.extraHeaders,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ClientConfig#ssl() ssl} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for ssl
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableClientConfig withSsl(Optional<? extends SSLConfig> optional) {
    @Nullable SSLConfig value = optional.orElse(null);
    if (this.ssl == value) return this;
    return new ImmutableClientConfig(
        this.target,
        value,
        this.userAgent,
        this.overrideAuthority,
        this.extraHeaders,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ClientConfig#userAgent() userAgent} attribute.
   * @param value The value for userAgent
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withUserAgent(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "userAgent");
    if (Objects.equals(this.userAgent, newValue)) return this;
    return new ImmutableClientConfig(
        this.target,
        this.ssl,
        newValue,
        this.overrideAuthority,
        this.extraHeaders,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ClientConfig#userAgent() userAgent} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for userAgent
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withUserAgent(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.userAgent, value)) return this;
    return new ImmutableClientConfig(
        this.target,
        this.ssl,
        value,
        this.overrideAuthority,
        this.extraHeaders,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ClientConfig#overrideAuthority() overrideAuthority} attribute.
   * @param value The value for overrideAuthority
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withOverrideAuthority(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "overrideAuthority");
    if (Objects.equals(this.overrideAuthority, newValue)) return this;
    return new ImmutableClientConfig(this.target, this.ssl, this.userAgent, newValue, this.extraHeaders, this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ClientConfig#overrideAuthority() overrideAuthority} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for overrideAuthority
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withOverrideAuthority(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.overrideAuthority, value)) return this;
    return new ImmutableClientConfig(this.target, this.ssl, this.userAgent, value, this.extraHeaders, this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by replacing the {@link ClientConfig#extraHeaders() extraHeaders} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the extraHeaders map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableClientConfig withExtraHeaders(Map<String, ? extends String> entries) {
    if (this.extraHeaders == entries) return this;
    ImmutableMap<String, String> newValue = ImmutableMap.copyOf(entries);
    return new ImmutableClientConfig(
        this.target,
        this.ssl,
        this.userAgent,
        this.overrideAuthority,
        newValue,
        this.maxInboundMessageSize);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ClientConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for maxInboundMessageSize
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableClientConfig withMaxInboundMessageSize(int value) {
    if (this.maxInboundMessageSize == value) return this;
    return new ImmutableClientConfig(this.target, this.ssl, this.userAgent, this.overrideAuthority, this.extraHeaders, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableClientConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableClientConfig
        && equalTo(0, (ImmutableClientConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableClientConfig another) {
    return target.equals(another.target)
        && Objects.equals(ssl, another.ssl)
        && Objects.equals(userAgent, another.userAgent)
        && Objects.equals(overrideAuthority, another.overrideAuthority)
        && extraHeaders.equals(another.extraHeaders)
        && maxInboundMessageSize == another.maxInboundMessageSize;
  }

  /**
   * Computes a hash code from attributes: {@code target}, {@code ssl}, {@code userAgent}, {@code overrideAuthority}, {@code extraHeaders}, {@code maxInboundMessageSize}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + target.hashCode();
    h += (h << 5) + Objects.hashCode(ssl);
    h += (h << 5) + Objects.hashCode(userAgent);
    h += (h << 5) + Objects.hashCode(overrideAuthority);
    h += (h << 5) + extraHeaders.hashCode();
    h += (h << 5) + maxInboundMessageSize;
    return h;
  }

  /**
   * Prints the immutable value {@code ClientConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ClientConfig")
        .omitNullValues()
        .add("target", target)
        .add("ssl", ssl)
        .add("userAgent", userAgent)
        .add("overrideAuthority", overrideAuthority)
        .add("extraHeaders", extraHeaders)
        .add("maxInboundMessageSize", maxInboundMessageSize)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link ClientConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ClientConfig instance
   */
  public static ImmutableClientConfig copyOf(ClientConfig instance) {
    if (instance instanceof ImmutableClientConfig) {
      return (ImmutableClientConfig) instance;
    }
    return ImmutableClientConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableClientConfig ImmutableClientConfig}.
   * <pre>
   * ImmutableClientConfig.builder()
   *    .target(io.deephaven.uri.DeephavenTarget) // required {@link ClientConfig#target() target}
<<<<<<< HEAD
   *    .ssl(io.deephaven.ssl.config.SSLConfig) // optional {@link ClientConfig#ssl() ssl}
=======
   *    .ssl(SSLConfig) // optional {@link ClientConfig#ssl() ssl}
>>>>>>> main
   *    .userAgent(String) // optional {@link ClientConfig#userAgent() userAgent}
   *    .overrideAuthority(String) // optional {@link ClientConfig#overrideAuthority() overrideAuthority}
   *    .putExtraHeaders|putAllExtraHeaders(String =&gt; String) // {@link ClientConfig#extraHeaders() extraHeaders} mappings
   *    .maxInboundMessageSize(int) // optional {@link ClientConfig#maxInboundMessageSize() maxInboundMessageSize}
   *    .build();
   * </pre>
   * @return A new ImmutableClientConfig builder
   */
  public static ImmutableClientConfig.Builder builder() {
    return new ImmutableClientConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableClientConfig ImmutableClientConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ClientConfig", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ClientConfig.Builder {
    private static final long INIT_BIT_TARGET = 0x1L;
    private static final long OPT_BIT_MAX_INBOUND_MESSAGE_SIZE = 0x1L;
    private long initBits = 0x1L;
    private long optBits;

    private @Nullable DeephavenTarget target;
    private @Nullable SSLConfig ssl;
    private @Nullable String userAgent;
    private @Nullable String overrideAuthority;
    private ImmutableMap.Builder<String, String> extraHeaders = ImmutableMap.builder();
    private int maxInboundMessageSize;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ClientConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ClientConfig instance) {
      Objects.requireNonNull(instance, "instance");
      target(instance.target());
      Optional<SSLConfig> sslOptional = instance.ssl();
      if (sslOptional.isPresent()) {
        ssl(sslOptional);
      }
      Optional<String> userAgentOptional = instance.userAgent();
      if (userAgentOptional.isPresent()) {
        userAgent(userAgentOptional);
      }
      Optional<String> overrideAuthorityOptional = instance.overrideAuthority();
      if (overrideAuthorityOptional.isPresent()) {
        overrideAuthority(overrideAuthorityOptional);
      }
      putAllExtraHeaders(instance.extraHeaders());
      maxInboundMessageSize(instance.maxInboundMessageSize());
      return this;
    }

    /**
     * Initializes the value for the {@link ClientConfig#target() target} attribute.
     * @param target The value for target 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder target(DeephavenTarget target) {
      this.target = Objects.requireNonNull(target, "target");
      initBits &= ~INIT_BIT_TARGET;
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(SSLConfig ssl) {
      this.ssl = Objects.requireNonNull(ssl, "ssl");
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#ssl() ssl} to ssl.
     * @param ssl The value for ssl
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ssl(Optional<? extends SSLConfig> ssl) {
      this.ssl = ssl.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#userAgent() userAgent} to userAgent.
     * @param userAgent The value for userAgent
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder userAgent(String userAgent) {
      this.userAgent = Objects.requireNonNull(userAgent, "userAgent");
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#userAgent() userAgent} to userAgent.
     * @param userAgent The value for userAgent
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder userAgent(Optional<String> userAgent) {
      this.userAgent = userAgent.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#overrideAuthority() overrideAuthority} to overrideAuthority.
     * @param overrideAuthority The value for overrideAuthority
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder overrideAuthority(String overrideAuthority) {
      this.overrideAuthority = Objects.requireNonNull(overrideAuthority, "overrideAuthority");
      return this;
    }

    /**
     * Initializes the optional value {@link ClientConfig#overrideAuthority() overrideAuthority} to overrideAuthority.
     * @param overrideAuthority The value for overrideAuthority
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder overrideAuthority(Optional<String> overrideAuthority) {
      this.overrideAuthority = overrideAuthority.orElse(null);
      return this;
    }

    /**
     * Put one entry to the {@link ClientConfig#extraHeaders() extraHeaders} map.
     * @param key The key in the extraHeaders map
     * @param value The associated value in the extraHeaders map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putExtraHeaders(String key, String value) {
      this.extraHeaders.put(key, value);
      return this;
    }

    /**
     * Put one entry to the {@link ClientConfig#extraHeaders() extraHeaders} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putExtraHeaders(Map.Entry<String, ? extends String> entry) {
      this.extraHeaders.put(entry);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link ClientConfig#extraHeaders() extraHeaders} map. Nulls are not permitted
     * @param entries The entries that will be added to the extraHeaders map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder extraHeaders(Map<String, ? extends String> entries) {
      this.extraHeaders = ImmutableMap.builder();
      return putAllExtraHeaders(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link ClientConfig#extraHeaders() extraHeaders} map. Nulls are not permitted
     * @param entries The entries that will be added to the extraHeaders map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllExtraHeaders(Map<String, ? extends String> entries) {
      this.extraHeaders.putAll(entries);
      return this;
    }

    /**
     * Initializes the value for the {@link ClientConfig#maxInboundMessageSize() maxInboundMessageSize} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ClientConfig#maxInboundMessageSize() maxInboundMessageSize}.</em>
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
     * Builds a new {@link ImmutableClientConfig ImmutableClientConfig}.
     * @return An immutable instance of ClientConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableClientConfig build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableClientConfig(this);
    }

    private boolean maxInboundMessageSizeIsSet() {
      return (optBits & OPT_BIT_MAX_INBOUND_MESSAGE_SIZE) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TARGET) != 0) attributes.add("target");
      return "Cannot build ClientConfig, some of required attributes are not set " + attributes;
    }
  }
}
