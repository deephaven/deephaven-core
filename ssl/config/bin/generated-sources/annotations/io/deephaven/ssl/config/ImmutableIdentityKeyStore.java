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
 * Immutable implementation of {@link IdentityKeyStore}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableIdentityKeyStore.builder()}.
 */
@Generated(from = "IdentityKeyStore", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableIdentityKeyStore extends IdentityKeyStore {
  private final String path;
  private final String password;
  private final String keystoreType;

  private ImmutableIdentityKeyStore(String path, String password, String keystoreType) {
    this.path = path;
    this.password = password;
    this.keystoreType = keystoreType;
  }

  /**
   * The keystore path.
   */
  @JsonProperty("path")
  @Override
  public String path() {
    return path;
  }

  /**
   * The keystore password.
   */
  @JsonProperty("password")
  @Override
  public String password() {
    return password;
  }

  /**
   * The optional keystore type.
   */
  @JsonProperty("keystoreType")
  @Override
  public Optional<String> keystoreType() {
    return Optional.ofNullable(keystoreType);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link IdentityKeyStore#path() path} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableIdentityKeyStore withPath(String value) {
    String newValue = Objects.requireNonNull(value, "path");
    if (this.path.equals(newValue)) return this;
    return new ImmutableIdentityKeyStore(newValue, this.password, this.keystoreType);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link IdentityKeyStore#password() password} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for password
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableIdentityKeyStore withPassword(String value) {
    String newValue = Objects.requireNonNull(value, "password");
    if (this.password.equals(newValue)) return this;
    return new ImmutableIdentityKeyStore(this.path, newValue, this.keystoreType);
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link IdentityKeyStore#keystoreType() keystoreType} attribute.
   * @param value The value for keystoreType
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityKeyStore withKeystoreType(String value) {
    String newValue = Objects.requireNonNull(value, "keystoreType");
    if (Objects.equals(this.keystoreType, newValue)) return this;
    return new ImmutableIdentityKeyStore(this.path, this.password, newValue);
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link IdentityKeyStore#keystoreType() keystoreType} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for keystoreType
   * @return A modified copy of {@code this} object
   */
  public final ImmutableIdentityKeyStore withKeystoreType(Optional<String> optional) {
    String value = optional.orElse(null);
    if (Objects.equals(this.keystoreType, value)) return this;
    return new ImmutableIdentityKeyStore(this.path, this.password, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableIdentityKeyStore} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableIdentityKeyStore
        && equalTo(0, (ImmutableIdentityKeyStore) another);
  }

  private boolean equalTo(int synthetic, ImmutableIdentityKeyStore another) {
    return path.equals(another.path)
        && password.equals(another.password)
        && Objects.equals(keystoreType, another.keystoreType);
  }

  /**
   * Computes a hash code from attributes: {@code path}, {@code password}, {@code keystoreType}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + path.hashCode();
    h += (h << 5) + password.hashCode();
    h += (h << 5) + Objects.hashCode(keystoreType);
    return h;
  }

  /**
   * Prints the immutable value {@code IdentityKeyStore} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("IdentityKeyStore{");
    builder.append("path=").append(path);
    builder.append(", ");
    builder.append("password=").append(password);
    if (keystoreType != null) {
      builder.append(", ");
      builder.append("keystoreType=").append(keystoreType);
    }
    return builder.append("}").toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "IdentityKeyStore", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends IdentityKeyStore {
    String path;
    String password;
    Optional<String> keystoreType = Optional.empty();
    @JsonProperty("path")
    public void setPath(String path) {
      this.path = path;
    }
    @JsonProperty("password")
    public void setPassword(String password) {
      this.password = password;
    }
    @JsonProperty("keystoreType")
    public void setKeystoreType(Optional<String> keystoreType) {
      this.keystoreType = keystoreType;
    }
    @Override
    public String path() { throw new UnsupportedOperationException(); }
    @Override
    public String password() { throw new UnsupportedOperationException(); }
    @Override
    public Optional<String> keystoreType() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableIdentityKeyStore fromJson(Json json) {
    ImmutableIdentityKeyStore.Builder builder = ImmutableIdentityKeyStore.builder();
    if (json.path != null) {
      builder.path(json.path);
    }
    if (json.password != null) {
      builder.password(json.password);
    }
    if (json.keystoreType != null) {
      builder.keystoreType(json.keystoreType);
    }
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link IdentityKeyStore} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable IdentityKeyStore instance
   */
  public static ImmutableIdentityKeyStore copyOf(IdentityKeyStore instance) {
    if (instance instanceof ImmutableIdentityKeyStore) {
      return (ImmutableIdentityKeyStore) instance;
    }
    return ImmutableIdentityKeyStore.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableIdentityKeyStore ImmutableIdentityKeyStore}.
   * <pre>
   * ImmutableIdentityKeyStore.builder()
   *    .path(String) // required {@link IdentityKeyStore#path() path}
   *    .password(String) // required {@link IdentityKeyStore#password() password}
   *    .keystoreType(String) // optional {@link IdentityKeyStore#keystoreType() keystoreType}
   *    .build();
   * </pre>
   * @return A new ImmutableIdentityKeyStore builder
   */
  public static ImmutableIdentityKeyStore.Builder builder() {
    return new ImmutableIdentityKeyStore.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableIdentityKeyStore ImmutableIdentityKeyStore}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "IdentityKeyStore", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private static final long INIT_BIT_PASSWORD = 0x2L;
    private long initBits = 0x3L;

    private String path;
    private String password;
    private String keystoreType;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code IdentityKeyStore} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(IdentityKeyStore instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      password(instance.password());
      Optional<String> keystoreTypeOptional = instance.keystoreType();
      if (keystoreTypeOptional.isPresent()) {
        keystoreType(keystoreTypeOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link IdentityKeyStore#path() path} attribute.
     * @param path The value for path 
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("path")
    public final Builder path(String path) {
      this.path = Objects.requireNonNull(path, "path");
      initBits &= ~INIT_BIT_PATH;
      return this;
    }

    /**
     * Initializes the value for the {@link IdentityKeyStore#password() password} attribute.
     * @param password The value for password 
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("password")
    public final Builder password(String password) {
      this.password = Objects.requireNonNull(password, "password");
      initBits &= ~INIT_BIT_PASSWORD;
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityKeyStore#keystoreType() keystoreType} to keystoreType.
     * @param keystoreType The value for keystoreType
     * @return {@code this} builder for chained invocation
     */
    public final Builder keystoreType(String keystoreType) {
      this.keystoreType = Objects.requireNonNull(keystoreType, "keystoreType");
      return this;
    }

    /**
     * Initializes the optional value {@link IdentityKeyStore#keystoreType() keystoreType} to keystoreType.
     * @param keystoreType The value for keystoreType
     * @return {@code this} builder for use in a chained invocation
     */
    @JsonProperty("keystoreType")
    public final Builder keystoreType(Optional<String> keystoreType) {
      this.keystoreType = keystoreType.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableIdentityKeyStore ImmutableIdentityKeyStore}.
     * @return An immutable instance of IdentityKeyStore
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableIdentityKeyStore build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableIdentityKeyStore(path, password, keystoreType);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      if ((initBits & INIT_BIT_PASSWORD) != 0) attributes.add("password");
      return "Cannot build IdentityKeyStore, some of required attributes are not set " + attributes;
    }
  }
}
