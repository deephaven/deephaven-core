package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SSLConfig}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSSLConfig.builder()}.
 */
@Generated(from = "SSLConfig", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableSSLConfig extends SSLConfig {
  private final Identity identity;
  private final Trust trust;
  private final Protocols protocols;
  private final Ciphers ciphers;
  private final SSLConfig.ClientAuth clientAuthentication;

  private ImmutableSSLConfig(
      Identity identity,
      Trust trust,
      Protocols protocols,
      Ciphers ciphers,
      SSLConfig.ClientAuth clientAuthentication) {
    this.identity = identity;
    this.trust = trust;
    this.protocols = protocols;
    this.ciphers = ciphers;
    this.clientAuthentication = clientAuthentication;
  }

  /**
   * The identity material. Optional for clients, necessary for servers.
   */
  @JsonProperty("identity")
  @Override
  public Optional<Identity> identity() {
    return Optional.ofNullable(identity);
  }

  /**
   * The optional trust material.
   */
  @JsonProperty("trust")
  @Override
  public Optional<Trust> trust() {
    return Optional.ofNullable(trust);
  }

  /**
   * The optional protocols.
   */
  @JsonProperty("protocols")
  @Override
  public Optional<Protocols> protocols() {
    return Optional.ofNullable(protocols);
  }

  /**
   * The optional ciphers.
   */
  @JsonProperty("ciphers")
  @Override
  public Optional<Ciphers> ciphers() {
    return Optional.ofNullable(ciphers);
  }

  /**
   * The optional client authentication.
   */
  @JsonProperty("clientAuthentication")
  @Override
  public Optional<SSLConfig.ClientAuth> clientAuthentication() {
    return Optional.ofNullable(clientAuthentication);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SSLConfig#identity() identity} attribute.
   * @param value The value for identity
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSSLConfig withIdentity(Identity value) {
    Identity newValue = Objects.requireNonNull(value, "identity");
    if (this.identity == newValue) return this;
    return validate(new ImmutableSSLConfig(newValue, this.trust, this.protocols, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SSLConfig#identity() identity} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for identity
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSSLConfig withIdentity(Optional<? extends Identity> optional) {
    Identity value = optional.orElse(null);
    if (this.identity == value) return this;
    return validate(new ImmutableSSLConfig(value, this.trust, this.protocols, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SSLConfig#trust() trust} attribute.
   * @param value The value for trust
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSSLConfig withTrust(Trust value) {
    Trust newValue = Objects.requireNonNull(value, "trust");
    if (this.trust == newValue) return this;
    return validate(new ImmutableSSLConfig(this.identity, newValue, this.protocols, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SSLConfig#trust() trust} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for trust
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSSLConfig withTrust(Optional<? extends Trust> optional) {
    Trust value = optional.orElse(null);
    if (this.trust == value) return this;
    return validate(new ImmutableSSLConfig(this.identity, value, this.protocols, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SSLConfig#protocols() protocols} attribute.
   * @param value The value for protocols
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSSLConfig withProtocols(Protocols value) {
    Protocols newValue = Objects.requireNonNull(value, "protocols");
    if (this.protocols == newValue) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, newValue, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SSLConfig#protocols() protocols} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for protocols
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSSLConfig withProtocols(Optional<? extends Protocols> optional) {
    Protocols value = optional.orElse(null);
    if (this.protocols == value) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, value, this.ciphers, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SSLConfig#ciphers() ciphers} attribute.
   * @param value The value for ciphers
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSSLConfig withCiphers(Ciphers value) {
    Ciphers newValue = Objects.requireNonNull(value, "ciphers");
    if (this.ciphers == newValue) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, this.protocols, newValue, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SSLConfig#ciphers() ciphers} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for ciphers
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSSLConfig withCiphers(Optional<? extends Ciphers> optional) {
    Ciphers value = optional.orElse(null);
    if (this.ciphers == value) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, this.protocols, value, this.clientAuthentication));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link SSLConfig#clientAuthentication() clientAuthentication} attribute.
   * @param value The value for clientAuthentication
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSSLConfig withClientAuthentication(SSLConfig.ClientAuth value) {
    SSLConfig.ClientAuth newValue = Objects.requireNonNull(value, "clientAuthentication");
    if (this.clientAuthentication == newValue) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, this.protocols, this.ciphers, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link SSLConfig#clientAuthentication() clientAuthentication} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for clientAuthentication
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableSSLConfig withClientAuthentication(Optional<? extends SSLConfig.ClientAuth> optional) {
    SSLConfig.ClientAuth value = optional.orElse(null);
    if (this.clientAuthentication == value) return this;
    return validate(new ImmutableSSLConfig(this.identity, this.trust, this.protocols, this.ciphers, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSSLConfig} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSSLConfig
        && equalTo(0, (ImmutableSSLConfig) another);
  }

  private boolean equalTo(int synthetic, ImmutableSSLConfig another) {
    return Objects.equals(identity, another.identity)
        && Objects.equals(trust, another.trust)
        && Objects.equals(protocols, another.protocols)
        && Objects.equals(ciphers, another.ciphers)
        && Objects.equals(clientAuthentication, another.clientAuthentication);
  }

  /**
   * Computes a hash code from attributes: {@code identity}, {@code trust}, {@code protocols}, {@code ciphers}, {@code clientAuthentication}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(identity);
    h += (h << 5) + Objects.hashCode(trust);
    h += (h << 5) + Objects.hashCode(protocols);
    h += (h << 5) + Objects.hashCode(ciphers);
    h += (h << 5) + Objects.hashCode(clientAuthentication);
    return h;
  }

  /**
   * Prints the immutable value {@code SSLConfig} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("SSLConfig{");
    if (identity != null) {
      builder.append("identity=").append(identity);
    }
    if (trust != null) {
      if (builder.length() > 10) builder.append(", ");
      builder.append("trust=").append(trust);
    }
    if (protocols != null) {
      if (builder.length() > 10) builder.append(", ");
      builder.append("protocols=").append(protocols);
    }
    if (ciphers != null) {
      if (builder.length() > 10) builder.append(", ");
      builder.append("ciphers=").append(ciphers);
    }
    if (clientAuthentication != null) {
      if (builder.length() > 10) builder.append(", ");
      builder.append("clientAuthentication=").append(clientAuthentication);
    }
    return builder.append("}").toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "SSLConfig", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends SSLConfig {
    Optional<Identity> identity = Optional.empty();
    Optional<Trust> trust = Optional.empty();
    Optional<Protocols> protocols = Optional.empty();
    Optional<Ciphers> ciphers = Optional.empty();
    Optional<SSLConfig.ClientAuth> clientAuthentication = Optional.empty();
    @JsonProperty("identity")
    public void setIdentity(Optional<Identity> identity) {
      this.identity = identity;
    }
    @JsonProperty("trust")
    public void setTrust(Optional<Trust> trust) {
      this.trust = trust;
    }
    @JsonProperty("protocols")
    public void setProtocols(Optional<Protocols> protocols) {
      this.protocols = protocols;
    }
    @JsonProperty("ciphers")
    public void setCiphers(Optional<Ciphers> ciphers) {
      this.ciphers = ciphers;
    }
    @JsonProperty("clientAuthentication")
    public void setClientAuthentication(Optional<SSLConfig.ClientAuth> clientAuthentication) {
      this.clientAuthentication = clientAuthentication;
    }
    @Override
    public SSLConfig withProtocols(Protocols protocols) { throw new UnsupportedOperationException(); }
    @Override
    public SSLConfig withCiphers(Ciphers ciphers) { throw new UnsupportedOperationException(); }
    @Override
    public SSLConfig withTrust(Trust trust) { throw new UnsupportedOperationException(); }
    @Override
    public Optional<Identity> identity() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<Trust> trust() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<Protocols> protocols() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<Ciphers> ciphers() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<SSLConfig.ClientAuth> clientAuthentication() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableSSLConfig fromJson(Json json) {
    ImmutableSSLConfig.Builder builder = ImmutableSSLConfig.builder();
    if (json.identity != null) {
      builder.identity(json.identity);
    }
    if (json.trust != null) {
      builder.trust(json.trust);
    }
    if (json.protocols != null) {
      builder.protocols(json.protocols);
    }
    if (json.ciphers != null) {
      builder.ciphers(json.ciphers);
    }
    if (json.clientAuthentication != null) {
      builder.clientAuthentication(json.clientAuthentication);
    }
    return builder.build();
  }

  private static ImmutableSSLConfig validate(ImmutableSSLConfig instance) {
    instance.checkMutualTLS();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link SSLConfig} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SSLConfig instance
   */
  public static ImmutableSSLConfig copyOf(SSLConfig instance) {
    if (instance instanceof ImmutableSSLConfig) {
      return (ImmutableSSLConfig) instance;
    }
    return ImmutableSSLConfig.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSSLConfig ImmutableSSLConfig}.
   * <pre>
   * ImmutableSSLConfig.builder()
<<<<<<< HEAD
   *    .identity(io.deephaven.ssl.config.Identity) // optional {@link SSLConfig#identity() identity}
   *    .trust(io.deephaven.ssl.config.Trust) // optional {@link SSLConfig#trust() trust}
   *    .protocols(io.deephaven.ssl.config.Protocols) // optional {@link SSLConfig#protocols() protocols}
   *    .ciphers(io.deephaven.ssl.config.Ciphers) // optional {@link SSLConfig#ciphers() ciphers}
=======
   *    .identity(Identity) // optional {@link SSLConfig#identity() identity}
   *    .trust(Trust) // optional {@link SSLConfig#trust() trust}
   *    .protocols(Protocols) // optional {@link SSLConfig#protocols() protocols}
   *    .ciphers(Ciphers) // optional {@link SSLConfig#ciphers() ciphers}
>>>>>>> main
   *    .clientAuthentication(io.deephaven.ssl.config.SSLConfig.ClientAuth) // optional {@link SSLConfig#clientAuthentication() clientAuthentication}
   *    .build();
   * </pre>
   * @return A new ImmutableSSLConfig builder
   */
  public static ImmutableSSLConfig.Builder builder() {
    return new ImmutableSSLConfig.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSSLConfig ImmutableSSLConfig}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SSLConfig", generator = "Immutables")
  public static final class Builder implements SSLConfig.Builder {
    private Identity identity;
    private Trust trust;
    private Protocols protocols;
    private Ciphers ciphers;
    private SSLConfig.ClientAuth clientAuthentication;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SSLConfig} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SSLConfig instance) {
      Objects.requireNonNull(instance, "instance");
      Optional<Identity> identityOptional = instance.identity();
      if (identityOptional.isPresent()) {
        identity(identityOptional);
      }
      Optional<Trust> trustOptional = instance.trust();
      if (trustOptional.isPresent()) {
        trust(trustOptional);
      }
      Optional<Protocols> protocolsOptional = instance.protocols();
      if (protocolsOptional.isPresent()) {
        protocols(protocolsOptional);
      }
      Optional<Ciphers> ciphersOptional = instance.ciphers();
      if (ciphersOptional.isPresent()) {
        ciphers(ciphersOptional);
      }
      Optional<SSLConfig.ClientAuth> clientAuthenticationOptional = instance.clientAuthentication();
      if (clientAuthenticationOptional.isPresent()) {
        clientAuthentication(clientAuthenticationOptional);
      }
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#identity() identity} to identity.
     * @param identity The value for identity
     * @return {@code this} builder for chained invocation
     */
    public final Builder identity(Identity identity) {
      this.identity = Objects.requireNonNull(identity, "identity");
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#identity() identity} to identity.
     * @param identity The value for identity
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("identity")
    public final Builder identity(Optional<? extends Identity> identity) {
      this.identity = identity.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#trust() trust} to trust.
     * @param trust The value for trust
     * @return {@code this} builder for chained invocation
     */
    public final Builder trust(Trust trust) {
      this.trust = Objects.requireNonNull(trust, "trust");
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#trust() trust} to trust.
     * @param trust The value for trust
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("trust")
    public final Builder trust(Optional<? extends Trust> trust) {
      this.trust = trust.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#protocols() protocols} to protocols.
     * @param protocols The value for protocols
     * @return {@code this} builder for chained invocation
     */
    public final Builder protocols(Protocols protocols) {
      this.protocols = Objects.requireNonNull(protocols, "protocols");
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#protocols() protocols} to protocols.
     * @param protocols The value for protocols
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("protocols")
    public final Builder protocols(Optional<? extends Protocols> protocols) {
      this.protocols = protocols.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#ciphers() ciphers} to ciphers.
     * @param ciphers The value for ciphers
     * @return {@code this} builder for chained invocation
     */
    public final Builder ciphers(Ciphers ciphers) {
      this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#ciphers() ciphers} to ciphers.
     * @param ciphers The value for ciphers
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("ciphers")
    public final Builder ciphers(Optional<? extends Ciphers> ciphers) {
      this.ciphers = ciphers.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#clientAuthentication() clientAuthentication} to clientAuthentication.
     * @param clientAuthentication The value for clientAuthentication
     * @return {@code this} builder for chained invocation
     */
    public final Builder clientAuthentication(SSLConfig.ClientAuth clientAuthentication) {
      this.clientAuthentication = Objects.requireNonNull(clientAuthentication, "clientAuthentication");
      return this;
    }

    /**
     * Initializes the optional value {@link SSLConfig#clientAuthentication() clientAuthentication} to clientAuthentication.
     * @param clientAuthentication The value for clientAuthentication
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("clientAuthentication")
    public final Builder clientAuthentication(Optional<? extends SSLConfig.ClientAuth> clientAuthentication) {
      this.clientAuthentication = clientAuthentication.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableSSLConfig ImmutableSSLConfig}.
     * @return An immutable instance of SSLConfig
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSSLConfig build() {
      return ImmutableSSLConfig.validate(new ImmutableSSLConfig(identity, trust, protocols, ciphers, clientAuthentication));
    }
  }
}
