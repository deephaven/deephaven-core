package io.deephaven.uri;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link DeephavenTarget}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableDeephavenTarget.builder()}.
 */
@Generated(from = "DeephavenTarget", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableDeephavenTarget extends DeephavenTarget {
  private final boolean isSecure;
  private final String host;
  private final Integer port;

  private ImmutableDeephavenTarget(boolean isSecure, String host, Integer port) {
    this.isSecure = isSecure;
    this.host = host;
    this.port = port;
  }

  /**
   * The secure flag, typically representing Transport Layer Security (TLS).
   * @return true if secure
   */
  @Override
  public boolean isSecure() {
    return isSecure;
  }

  /**
   * The host or IP address.
   * @return the host
   */
  @Override
  public String host() {
    return host;
  }

  /**
   * The optional port.
   * @return the port
   */
  @Override
  public OptionalInt port() {
    return port != null
        ? OptionalInt.of(port)
        : OptionalInt.empty();
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenTarget#isSecure() isSecure} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isSecure
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenTarget withIsSecure(boolean value) {
    if (this.isSecure == value) return this;
    return validate(new ImmutableDeephavenTarget(value, this.host, this.port));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link DeephavenTarget#host() host} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for host
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableDeephavenTarget withHost(String value) {
    String newValue = Objects.requireNonNull(value, "host");
    if (this.host.equals(newValue)) return this;
    return validate(new ImmutableDeephavenTarget(this.isSecure, newValue, this.port));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link DeephavenTarget#port() port} attribute.
   * @param value The value for port
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenTarget withPort(int value) {
    Integer newValue = value;
    if (Objects.equals(this.port, newValue)) return this;
    return validate(new ImmutableDeephavenTarget(this.isSecure, this.host, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link DeephavenTarget#port() port} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for port
   * @return A modified copy of {@code this} object
   */
  public final ImmutableDeephavenTarget withPort(OptionalInt optional) {
    Integer value = optional.isPresent() ? optional.getAsInt() : null;
    if (Objects.equals(this.port, value)) return this;
    return validate(new ImmutableDeephavenTarget(this.isSecure, this.host, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableDeephavenTarget} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableDeephavenTarget
        && equalTo(0, (ImmutableDeephavenTarget) another);
  }

  private boolean equalTo(int synthetic, ImmutableDeephavenTarget another) {
    return isSecure == another.isSecure
        && host.equals(another.host)
        && Objects.equals(port, another.port);
  }

  /**
   * Computes a hash code from attributes: {@code isSecure}, {@code host}, {@code port}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(isSecure);
    h += (h << 5) + host.hashCode();
    h += (h << 5) + Objects.hashCode(port);
    return h;
  }

  private static ImmutableDeephavenTarget validate(ImmutableDeephavenTarget instance) {
    instance.checkHostPort();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link DeephavenTarget} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable DeephavenTarget instance
   */
  public static ImmutableDeephavenTarget copyOf(DeephavenTarget instance) {
    if (instance instanceof ImmutableDeephavenTarget) {
      return (ImmutableDeephavenTarget) instance;
    }
    return ImmutableDeephavenTarget.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableDeephavenTarget ImmutableDeephavenTarget}.
   * <pre>
   * ImmutableDeephavenTarget.builder()
   *    .isSecure(boolean) // required {@link DeephavenTarget#isSecure() isSecure}
   *    .host(String) // required {@link DeephavenTarget#host() host}
   *    .port(int) // optional {@link DeephavenTarget#port() port}
   *    .build();
   * </pre>
   * @return A new ImmutableDeephavenTarget builder
   */
  public static ImmutableDeephavenTarget.Builder builder() {
    return new ImmutableDeephavenTarget.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableDeephavenTarget ImmutableDeephavenTarget}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "DeephavenTarget", generator = "Immutables")
  public static final class Builder implements DeephavenTarget.Builder {
    private static final long INIT_BIT_IS_SECURE = 0x1L;
    private static final long INIT_BIT_HOST = 0x2L;
    private long initBits = 0x3L;

    private boolean isSecure;
    private String host;
    private Integer port;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code DeephavenTarget} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(DeephavenTarget instance) {
      Objects.requireNonNull(instance, "instance");
      isSecure(instance.isSecure());
      host(instance.host());
      OptionalInt portOptional = instance.port();
      if (portOptional.isPresent()) {
        port(portOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenTarget#isSecure() isSecure} attribute.
     * @param isSecure The value for isSecure 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder isSecure(boolean isSecure) {
      this.isSecure = isSecure;
      initBits &= ~INIT_BIT_IS_SECURE;
      return this;
    }

    /**
     * Initializes the value for the {@link DeephavenTarget#host() host} attribute.
     * @param host The value for host 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder host(String host) {
      this.host = Objects.requireNonNull(host, "host");
      initBits &= ~INIT_BIT_HOST;
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenTarget#port() port} to port.
     * @param port The value for port
     * @return {@code this} builder for chained invocation
     */
    public final Builder port(int port) {
      this.port = port;
      return this;
    }

    /**
     * Initializes the optional value {@link DeephavenTarget#port() port} to port.
     * @param port The value for port
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder port(OptionalInt port) {
      this.port = port.isPresent() ? port.getAsInt() : null;
      return this;
    }

    /**
     * Builds a new {@link ImmutableDeephavenTarget ImmutableDeephavenTarget}.
     * @return An immutable instance of DeephavenTarget
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableDeephavenTarget build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableDeephavenTarget.validate(new ImmutableDeephavenTarget(isSecure, host, port));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_IS_SECURE) != 0) attributes.add("isSecure");
      if ((initBits & INIT_BIT_HOST) != 0) attributes.add("host");
      return "Cannot build DeephavenTarget, some of required attributes are not set " + attributes;
    }
  }
}
