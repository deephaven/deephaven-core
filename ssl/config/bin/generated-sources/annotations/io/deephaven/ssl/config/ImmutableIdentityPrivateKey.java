package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link IdentityPrivateKey}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIdentityPrivateKey.builder()}.
 */
@Generated(from = "IdentityPrivateKey", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableIdentityPrivateKey extends IdentityPrivateKey {
  private final String certChainPath;
  private final String privateKeyPath;
  private final String privateKeyPassword;
  private final String alias;

  private ImmutableIdentityPrivateKey(
      String certChainPath,
      String privateKeyPath,
      String privateKeyPassword,
      String alias) {
    this.certChainPath = certChainPath;
    this.privateKeyPath = privateKeyPath;
    this.privateKeyPassword = privateKeyPassword;
    this.alias = alias;
  }

  /**
   * The certificate chain path.
   */
  @JsonProperty("certChainPath")
  @Override
  public String certChainPath() {
    return certChainPath;
  }

  /**
   * The private key path.
   */
  @JsonProperty("privateKeyPath")
  @Override
  public String privateKeyPath() {
    return privateKeyPath;
  }

  /**
   * The optional private key password.
   */
  @JsonProperty("privateKeyPassword")
  @Override
  public Optional<String> privateKeyPassword() {
    return Optional.ofNullable(privateKeyPassword);
  }

  /**
   * The optional alias.
   */
  @JsonProperty("alias")
  @Override
  public Optional<String> alias() {
    return Optional.ofNullable(alias);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link IdentityPrivateKey#certChainPath() certChainPath} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for certChainPath
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableIdentityPrivateKey withCertChainPath(String value) {
    String newValue = Objects.requireNonNull(value, "certChainPath");
    if (this.certChainPath.equals(newValue)) return this;
    return new ImmutableIdentityPrivateKey(newValue, this.privateKeyPath, this.privateKeyPassword, this.alias);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link IdentityPrivateKey#privateKeyPath() privateKeyPath} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for privateKeyPath
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableIdentityPrivateKey withPrivateKeyPath(String value) {
    String newValue = Objects.requireNonNull(value, "privateKeyPath");
    if (this.privateKeyPath.equals(newValue)) return this;
    return new ImmutableIdentityPrivateKey(this.certChainPath, newValue, this.privateKeyPassword, this.alias);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link IdentityPrivateKey#privateKeyPassword() privateKeyPassword} attribute.
   * @param value The value for privateKeyPassword
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityPrivateKey withPrivateKeyPassword(String value) {
    String newValue = Objects.requireNonNull(value, "privateKeyPassword");
    if (Objects.equals(this.privateKeyPassword, newValue)) return this;
    return new ImmutableIdentityPrivateKey(this.certChainPath, this.privateKeyPath, newValue, this.alias);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link IdentityPrivateKey#privateKeyPassword() privateKeyPassword} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for privateKeyPassword
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityPrivateKey withPrivateKeyPassword(Optional<String> optional) {
    String value = optional.orElse(null);
    if (Objects.equals(this.privateKeyPassword, value)) return this;
    return new ImmutableIdentityPrivateKey(this.certChainPath, this.privateKeyPath, value, this.alias);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link IdentityPrivateKey#alias() alias} attribute.
   * @param value The value for alias
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityPrivateKey withAlias(String value) {
    String newValue = Objects.requireNonNull(value, "alias");
    if (Objects.equals(this.alias, newValue)) return this;
    return new ImmutableIdentityPrivateKey(this.certChainPath, this.privateKeyPath, this.privateKeyPassword, newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link IdentityPrivateKey#alias() alias} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for alias
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityPrivateKey withAlias(Optional<String> optional) {
    String value = optional.orElse(null);
    if (Objects.equals(this.alias, value)) return this;
    return new ImmutableIdentityPrivateKey(this.certChainPath, this.privateKeyPath, this.privateKeyPassword, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIdentityPrivateKey} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIdentityPrivateKey
        && equalTo(0, (ImmutableIdentityPrivateKey) another);
  }

  private boolean equalTo(int synthetic, ImmutableIdentityPrivateKey another) {
    return certChainPath.equals(another.certChainPath)
        && privateKeyPath.equals(another.privateKeyPath)
        && Objects.equals(privateKeyPassword, another.privateKeyPassword)
        && Objects.equals(alias, another.alias);
  }

  /**
   * Computes a hash code from attributes: {@code certChainPath}, {@code privateKeyPath}, {@code privateKeyPassword}, {@code alias}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + certChainPath.hashCode();
    h += (h << 5) + privateKeyPath.hashCode();
    h += (h << 5) + Objects.hashCode(privateKeyPassword);
    h += (h << 5) + Objects.hashCode(alias);
    return h;
  }

  /**
   * Prints the immutable value {@code IdentityPrivateKey} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("IdentityPrivateKey{");
    builder.append("certChainPath=").append(certChainPath);
    builder.append(", ");
    builder.append("privateKeyPath=").append(privateKeyPath);
    if (privateKeyPassword != null) {
      builder.append(", ");
      builder.append("privateKeyPassword=").append(privateKeyPassword);
    }
    if (alias != null) {
      builder.append(", ");
      builder.append("alias=").append(alias);
    }
    return builder.append("}").toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "IdentityPrivateKey", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends IdentityPrivateKey {
    String certChainPath;
    String privateKeyPath;
    Optional<String> privateKeyPassword = Optional.empty();
    Optional<String> alias = Optional.empty();
    @JsonProperty("certChainPath")
    public void setCertChainPath(String certChainPath) {
      this.certChainPath = certChainPath;
    }
    @JsonProperty("privateKeyPath")
    public void setPrivateKeyPath(String privateKeyPath) {
      this.privateKeyPath = privateKeyPath;
    }
    @JsonProperty("privateKeyPassword")
    public void setPrivateKeyPassword(Optional<String> privateKeyPassword) {
      this.privateKeyPassword = privateKeyPassword;
    }
    @JsonProperty("alias")
    public void setAlias(Optional<String> alias) {
      this.alias = alias;
    }
    @Override
    public String certChainPath() { throw new UnsupportedOperationException(); }
    @Override
    public String privateKeyPath() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<String> privateKeyPassword() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<String> alias() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableIdentityPrivateKey fromJson(Json json) {
    ImmutableIdentityPrivateKey.Builder builder = ImmutableIdentityPrivateKey.builder();
    if (json.certChainPath != null) {
      builder.certChainPath(json.certChainPath);
    }
    if (json.privateKeyPath != null) {
      builder.privateKeyPath(json.privateKeyPath);
    }
    if (json.privateKeyPassword != null) {
      builder.privateKeyPassword(json.privateKeyPassword);
    }
    if (json.alias != null) {
      builder.alias(json.alias);
    }
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link IdentityPrivateKey} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IdentityPrivateKey instance
   */
  public static ImmutableIdentityPrivateKey copyOf(IdentityPrivateKey instance) {
    if (instance instanceof ImmutableIdentityPrivateKey) {
      return (ImmutableIdentityPrivateKey) instance;
    }
    return ImmutableIdentityPrivateKey.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIdentityPrivateKey ImmutableIdentityPrivateKey}.
   * <pre>
   * ImmutableIdentityPrivateKey.builder()
   *    .certChainPath(String) // required {@link IdentityPrivateKey#certChainPath() certChainPath}
   *    .privateKeyPath(String) // required {@link IdentityPrivateKey#privateKeyPath() privateKeyPath}
   *    .privateKeyPassword(String) // optional {@link IdentityPrivateKey#privateKeyPassword() privateKeyPassword}
   *    .alias(String) // optional {@link IdentityPrivateKey#alias() alias}
   *    .build();
   * </pre>
   * @return A new ImmutableIdentityPrivateKey builder
   */
  public static ImmutableIdentityPrivateKey.Builder builder() {
    return new ImmutableIdentityPrivateKey.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIdentityPrivateKey ImmutableIdentityPrivateKey}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IdentityPrivateKey", generator = "Immutables")
  public static final class Builder implements IdentityPrivateKey.Builder {
    private static final long INIT_BIT_CERT_CHAIN_PATH = 0x1L;
    private static final long INIT_BIT_PRIVATE_KEY_PATH = 0x2L;
    private long initBits = 0x3L;

    private String certChainPath;
    private String privateKeyPath;
    private String privateKeyPassword;
    private String alias;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IdentityPrivateKey} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(IdentityPrivateKey instance) {
      Objects.requireNonNull(instance, "instance");
      certChainPath(instance.certChainPath());
      privateKeyPath(instance.privateKeyPath());
      Optional<String> privateKeyPasswordOptional = instance.privateKeyPassword();
      if (privateKeyPasswordOptional.isPresent()) {
        privateKeyPassword(privateKeyPasswordOptional);
      }
      Optional<String> aliasOptional = instance.alias();
      if (aliasOptional.isPresent()) {
        alias(aliasOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link IdentityPrivateKey#certChainPath() certChainPath} attribute.
     * @param certChainPath The value for certChainPath 
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("certChainPath")
    public final Builder certChainPath(String certChainPath) {
      this.certChainPath = Objects.requireNonNull(certChainPath, "certChainPath");
      initBits &= ~INIT_BIT_CERT_CHAIN_PATH;
      return this;
    }

    /**
     * Initializes the value for the {@link IdentityPrivateKey#privateKeyPath() privateKeyPath} attribute.
     * @param privateKeyPath The value for privateKeyPath 
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("privateKeyPath")
    public final Builder privateKeyPath(String privateKeyPath) {
      this.privateKeyPath = Objects.requireNonNull(privateKeyPath, "privateKeyPath");
      initBits &= ~INIT_BIT_PRIVATE_KEY_PATH;
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityPrivateKey#privateKeyPassword() privateKeyPassword} to privateKeyPassword.
     * @param privateKeyPassword The value for privateKeyPassword
     * @return {@code this} builder for chained invocation
     */
    public final Builder privateKeyPassword(String privateKeyPassword) {
      this.privateKeyPassword = Objects.requireNonNull(privateKeyPassword, "privateKeyPassword");
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityPrivateKey#privateKeyPassword() privateKeyPassword} to privateKeyPassword.
     * @param privateKeyPassword The value for privateKeyPassword
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("privateKeyPassword")
    public final Builder privateKeyPassword(Optional<String> privateKeyPassword) {
      this.privateKeyPassword = privateKeyPassword.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityPrivateKey#alias() alias} to alias.
     * @param alias The value for alias
     * @return {@code this} builder for chained invocation
     */
    public final Builder alias(String alias) {
      this.alias = Objects.requireNonNull(alias, "alias");
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityPrivateKey#alias() alias} to alias.
     * @param alias The value for alias
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("alias")
    public final Builder alias(Optional<String> alias) {
      this.alias = alias.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableIdentityPrivateKey ImmutableIdentityPrivateKey}.
     * @return An immutable instance of IdentityPrivateKey
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIdentityPrivateKey build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableIdentityPrivateKey(certChainPath, privateKeyPath, privateKeyPassword, alias);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CERT_CHAIN_PATH) != 0) attributes.add("certChainPath");
      if ((initBits & INIT_BIT_PRIVATE_KEY_PATH) != 0) attributes.add("privateKeyPath");
      return "Cannot build IdentityPrivateKey, some of required attributes are not set " + attributes;
    }
  }
}
