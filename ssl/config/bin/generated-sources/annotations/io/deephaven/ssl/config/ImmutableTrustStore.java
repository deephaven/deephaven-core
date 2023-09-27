package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
<<<<<<< HEAD
import com.fasterxml.jackson.annotation.JsonTypeInfo;
=======
>>>>>>> main
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TrustStore}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTrustStore.builder()}.
 */
@Generated(from = "TrustStore", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTrustStore extends TrustStore {
  private final String path;
  private final String password;

  private ImmutableTrustStore(String path, String password) {
    this.path = path;
    this.password = password;
  }

  /**
   * The trust store path.
   */
  @JsonProperty("path")
  @Override
  public String path() {
    return path;
  }

  /**
   * The trust storce password.
   */
  @JsonProperty("password")
  @Override
  public String password() {
    return password;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TrustStore#path() path} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for path
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTrustStore withPath(String value) {
    String newValue = Objects.requireNonNull(value, "path");
    if (this.path.equals(newValue)) return this;
    return new ImmutableTrustStore(newValue, this.password);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TrustStore#password() password} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for password
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTrustStore withPassword(String value) {
    String newValue = Objects.requireNonNull(value, "password");
    if (this.password.equals(newValue)) return this;
    return new ImmutableTrustStore(this.path, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTrustStore} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTrustStore
        && equalTo(0, (ImmutableTrustStore) another);
  }

  private boolean equalTo(int synthetic, ImmutableTrustStore another) {
    return path.equals(another.path)
        && password.equals(another.password);
  }

  /**
   * Computes a hash code from attributes: {@code path}, {@code password}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + path.hashCode();
    h += (h << 5) + password.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TrustStore} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TrustStore{"
        + "path=" + path
        + ", password=" + password
        + "}";
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "TrustStore", generator = "Immutables")
  @Deprecated
  @JsonDeserialize
<<<<<<< HEAD
  @JsonTypeInfo(use=JsonTypeInfo.Id.NONE)
=======
>>>>>>> main
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends TrustStore {
    String path;
    String password;
    @JsonProperty("path")
    public void setPath(String path) {
      this.path = path;
    }
    @JsonProperty("password")
    public void setPassword(String password) {
      this.password = password;
    }
    @Override
    public String path() { throw new UnsupportedOperationException(); }
    @Override
    public String password() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static ImmutableTrustStore fromJson(Json json) {
    ImmutableTrustStore.Builder builder = ImmutableTrustStore.builder();
    if (json.path != null) {
      builder.path(json.path);
    }
    if (json.password != null) {
      builder.password(json.password);
    }
    return builder.build();
  }

  /**
   * Creates an immutable copy of a {@link TrustStore} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TrustStore instance
   */
  public static ImmutableTrustStore copyOf(TrustStore instance) {
    if (instance instanceof ImmutableTrustStore) {
      return (ImmutableTrustStore) instance;
    }
    return ImmutableTrustStore.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTrustStore ImmutableTrustStore}.
   * <pre>
   * ImmutableTrustStore.builder()
   *    .path(String) // required {@link TrustStore#path() path}
   *    .password(String) // required {@link TrustStore#password() password}
   *    .build();
   * </pre>
   * @return A new ImmutableTrustStore builder
   */
  public static ImmutableTrustStore.Builder builder() {
    return new ImmutableTrustStore.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTrustStore ImmutableTrustStore}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TrustStore", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PATH = 0x1L;
    private static final long INIT_BIT_PASSWORD = 0x2L;
    private long initBits = 0x3L;

    private String path;
    private String password;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TrustStore} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TrustStore instance) {
      Objects.requireNonNull(instance, "instance");
      path(instance.path());
      password(instance.password());
      return this;
    }

    /**
     * Initializes the value for the {@link TrustStore#path() path} attribute.
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
     * Initializes the value for the {@link TrustStore#password() password} attribute.
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
     * Builds a new {@link ImmutableTrustStore ImmutableTrustStore}.
     * @return An immutable instance of TrustStore
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTrustStore build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableTrustStore(path, password);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PATH) != 0) attributes.add("path");
      if ((initBits & INIT_BIT_PASSWORD) != 0) attributes.add("password");
      return "Cannot build TrustStore, some of required attributes are not set " + attributes;
    }
  }
}
