package io.deephaven.process;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link NetworkOshi}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableNetworkOshi.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableNetworkOshi.of()}.
 */
@Generated(from = "NetworkOshi", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableNetworkOshi extends NetworkOshi {
  private final String hostName;
  private final String domainName;
  private final DnsServers dnsServers;
  private final String ipv4DefaultGateway;
  private final String ipv6DefaultGateway;

  private ImmutableNetworkOshi(
      String hostName,
      Optional<String> domainName,
      DnsServers dnsServers,
      Optional<String> ipv4DefaultGateway,
      Optional<String> ipv6DefaultGateway) {
    this.hostName = Objects.requireNonNull(hostName, "hostName");
    this.domainName = domainName.orElse(null);
    this.dnsServers = Objects.requireNonNull(dnsServers, "dnsServers");
    this.ipv4DefaultGateway = ipv4DefaultGateway.orElse(null);
    this.ipv6DefaultGateway = ipv6DefaultGateway.orElse(null);
  }

  private ImmutableNetworkOshi(ImmutableNetworkOshi.Builder builder) {
    this.hostName = builder.hostName;
    this.domainName = builder.domainName;
    this.dnsServers = builder.dnsServers;
    this.ipv4DefaultGateway = builder.ipv4DefaultGateway;
    this.ipv6DefaultGateway = builder.ipv6DefaultGateway;
  }

  /**
   * @return The value of the {@code hostName} attribute
   */
  @Override
  public String getHostName() {
    return hostName;
  }

  /**
   * @return The value of the {@code domainName} attribute
   */
  @Override
  public Optional<String> getDomainName() {
    return Optional.ofNullable(domainName);
  }

  /**
   * @return The value of the {@code dnsServers} attribute
   */
  @Override
  public DnsServers getDnsServers() {
    return dnsServers;
  }

  /**
   * @return The value of the {@code ipv4DefaultGateway} attribute
   */
  @Override
  public Optional<String> getIpv4DefaultGateway() {
    return Optional.ofNullable(ipv4DefaultGateway);
  }

  /**
   * @return The value of the {@code ipv6DefaultGateway} attribute
   */
  @Override
  public Optional<String> getIpv6DefaultGateway() {
    return Optional.ofNullable(ipv6DefaultGateway);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableNetworkOshi} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableNetworkOshi
        && equalTo((ImmutableNetworkOshi) another);
  }

  private boolean equalTo(ImmutableNetworkOshi another) {
    return hostName.equals(another.hostName)
        && Objects.equals(domainName, another.domainName)
        && dnsServers.equals(another.dnsServers)
        && Objects.equals(ipv4DefaultGateway, another.ipv4DefaultGateway)
        && Objects.equals(ipv6DefaultGateway, another.ipv6DefaultGateway);
  }

  /**
   * Computes a hash code from attributes: {@code hostName}, {@code domainName}, {@code dnsServers}, {@code ipv4DefaultGateway}, {@code ipv6DefaultGateway}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + hostName.hashCode();
    h += (h << 5) + Objects.hashCode(domainName);
    h += (h << 5) + dnsServers.hashCode();
    h += (h << 5) + Objects.hashCode(ipv4DefaultGateway);
    h += (h << 5) + Objects.hashCode(ipv6DefaultGateway);
    return h;
  }


  /**
   * Prints the immutable value {@code NetworkOshi} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("NetworkOshi{");
    builder.append("hostName=").append(hostName);
    if (domainName != null) {
      builder.append(", ");
      builder.append("domainName=").append(domainName);
    }
    builder.append(", ");
    builder.append("dnsServers=").append(dnsServers);
    if (ipv4DefaultGateway != null) {
      builder.append(", ");
      builder.append("ipv4DefaultGateway=").append(ipv4DefaultGateway);
    }
    if (ipv6DefaultGateway != null) {
      builder.append(", ");
      builder.append("ipv6DefaultGateway=").append(ipv6DefaultGateway);
    }
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code NetworkOshi} instance.
   * @param hostName The value for the {@code hostName} attribute
   * @param domainName The value for the {@code domainName} attribute
   * @param dnsServers The value for the {@code dnsServers} attribute
   * @param ipv4DefaultGateway The value for the {@code ipv4DefaultGateway} attribute
   * @param ipv6DefaultGateway The value for the {@code ipv6DefaultGateway} attribute
   * @return An immutable NetworkOshi instance
   */
  public static ImmutableNetworkOshi of(String hostName, Optional<String> domainName, DnsServers dnsServers, Optional<String> ipv4DefaultGateway, Optional<String> ipv6DefaultGateway) {
    return new ImmutableNetworkOshi(hostName, domainName, dnsServers, ipv4DefaultGateway, ipv6DefaultGateway);
  }

  /**
   * Creates a builder for {@link ImmutableNetworkOshi ImmutableNetworkOshi}.
   * <pre>
   * ImmutableNetworkOshi.builder()
   *    .hostName(String) // required {@link NetworkOshi#getHostName() hostName}
   *    .domainName(String) // optional {@link NetworkOshi#getDomainName() domainName}
   *    .dnsServers(io.deephaven.process.DnsServers) // required {@link NetworkOshi#getDnsServers() dnsServers}
   *    .ipv4DefaultGateway(String) // optional {@link NetworkOshi#getIpv4DefaultGateway() ipv4DefaultGateway}
   *    .ipv6DefaultGateway(String) // optional {@link NetworkOshi#getIpv6DefaultGateway() ipv6DefaultGateway}
   *    .build();
   * </pre>
   * @return A new ImmutableNetworkOshi builder
   */
  public static ImmutableNetworkOshi.Builder builder() {
    return new ImmutableNetworkOshi.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableNetworkOshi ImmutableNetworkOshi}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "NetworkOshi", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_HOST_NAME = 0x1L;
    private static final long INIT_BIT_DNS_SERVERS = 0x2L;
    private static final long OPT_BIT_DOMAIN_NAME = 0x1L;
    private static final long OPT_BIT_IPV4_DEFAULT_GATEWAY = 0x2L;
    private static final long OPT_BIT_IPV6_DEFAULT_GATEWAY = 0x4L;
    private long initBits = 0x3L;
    private long optBits;

    private String hostName;
    private String domainName;
    private DnsServers dnsServers;
    private String ipv4DefaultGateway;
    private String ipv6DefaultGateway;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link NetworkOshi#getHostName() hostName} attribute.
     * @param hostName The value for hostName 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder hostName(String hostName) {
      checkNotIsSet(hostNameIsSet(), "hostName");
      this.hostName = Objects.requireNonNull(hostName, "hostName");
      initBits &= ~INIT_BIT_HOST_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getDomainName() domainName} to domainName.
     * @param domainName The value for domainName
     * @return {@code this} builder for chained invocation
     */
    public final Builder domainName(String domainName) {
      checkNotIsSet(domainNameIsSet(), "domainName");
      this.domainName = Objects.requireNonNull(domainName, "domainName");
      optBits |= OPT_BIT_DOMAIN_NAME;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getDomainName() domainName} to domainName.
     * @param domainName The value for domainName
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder domainName(Optional<String> domainName) {
      checkNotIsSet(domainNameIsSet(), "domainName");
      this.domainName = domainName.orElse(null);
      optBits |= OPT_BIT_DOMAIN_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link NetworkOshi#getDnsServers() dnsServers} attribute.
     * @param dnsServers The value for dnsServers 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder dnsServers(DnsServers dnsServers) {
      checkNotIsSet(dnsServersIsSet(), "dnsServers");
      this.dnsServers = Objects.requireNonNull(dnsServers, "dnsServers");
      initBits &= ~INIT_BIT_DNS_SERVERS;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getIpv4DefaultGateway() ipv4DefaultGateway} to ipv4DefaultGateway.
     * @param ipv4DefaultGateway The value for ipv4DefaultGateway
     * @return {@code this} builder for chained invocation
     */
    public final Builder ipv4DefaultGateway(String ipv4DefaultGateway) {
      checkNotIsSet(ipv4DefaultGatewayIsSet(), "ipv4DefaultGateway");
      this.ipv4DefaultGateway = Objects.requireNonNull(ipv4DefaultGateway, "ipv4DefaultGateway");
      optBits |= OPT_BIT_IPV4_DEFAULT_GATEWAY;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getIpv4DefaultGateway() ipv4DefaultGateway} to ipv4DefaultGateway.
     * @param ipv4DefaultGateway The value for ipv4DefaultGateway
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder ipv4DefaultGateway(Optional<String> ipv4DefaultGateway) {
      checkNotIsSet(ipv4DefaultGatewayIsSet(), "ipv4DefaultGateway");
      this.ipv4DefaultGateway = ipv4DefaultGateway.orElse(null);
      optBits |= OPT_BIT_IPV4_DEFAULT_GATEWAY;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getIpv6DefaultGateway() ipv6DefaultGateway} to ipv6DefaultGateway.
     * @param ipv6DefaultGateway The value for ipv6DefaultGateway
     * @return {@code this} builder for chained invocation
     */
    public final Builder ipv6DefaultGateway(String ipv6DefaultGateway) {
      checkNotIsSet(ipv6DefaultGatewayIsSet(), "ipv6DefaultGateway");
      this.ipv6DefaultGateway = Objects.requireNonNull(ipv6DefaultGateway, "ipv6DefaultGateway");
      optBits |= OPT_BIT_IPV6_DEFAULT_GATEWAY;
      return this;
    }

    /**
     * Initializes the optional value {@link NetworkOshi#getIpv6DefaultGateway() ipv6DefaultGateway} to ipv6DefaultGateway.
     * @param ipv6DefaultGateway The value for ipv6DefaultGateway
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder ipv6DefaultGateway(Optional<String> ipv6DefaultGateway) {
      checkNotIsSet(ipv6DefaultGatewayIsSet(), "ipv6DefaultGateway");
      this.ipv6DefaultGateway = ipv6DefaultGateway.orElse(null);
      optBits |= OPT_BIT_IPV6_DEFAULT_GATEWAY;
      return this;
    }

    /**
     * Builds a new {@link ImmutableNetworkOshi ImmutableNetworkOshi}.
     * @return An immutable instance of NetworkOshi
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableNetworkOshi build() {
      checkRequiredAttributes();
      return new ImmutableNetworkOshi(this);
    }

    private boolean domainNameIsSet() {
      return (optBits & OPT_BIT_DOMAIN_NAME) != 0;
    }

    private boolean ipv4DefaultGatewayIsSet() {
      return (optBits & OPT_BIT_IPV4_DEFAULT_GATEWAY) != 0;
    }

    private boolean ipv6DefaultGatewayIsSet() {
      return (optBits & OPT_BIT_IPV6_DEFAULT_GATEWAY) != 0;
    }

    private boolean hostNameIsSet() {
      return (initBits & INIT_BIT_HOST_NAME) == 0;
    }

    private boolean dnsServersIsSet() {
      return (initBits & INIT_BIT_DNS_SERVERS) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of NetworkOshi is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!hostNameIsSet()) attributes.add("hostName");
      if (!dnsServersIsSet()) attributes.add("dnsServers");
      return "Cannot build NetworkOshi, some of required attributes are not set " + attributes;
    }
  }
}
