package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.Arrays;
import java.util.Optional;
import org.immutables.value.Value;
import oshi.software.os.NetworkParams;

/**
 * NetworkParams presents network parameters of running OS, such as DNS, host name etc.
 */
@Value.Immutable
@ProcessStyle
public abstract class NetworkOshi implements PropertySet {

    private static final String HOSTNAME = "hostname";
    private static final String DOMAINNAME = "domainname";
    private static final String GATEWAY_IPV_4 = "gateway.ipv4";
    private static final String GATEWAY_IPV_6 = "gateway.ipv6";
    private static final String DNS = "dns";

    /**
     * <p>
     * getHostName.
     * </p>
     *
     * @return Gets host name
     */
    @Value.Parameter
    public abstract String getHostName();

    /**
     * <p>
     * getDomainName.
     * </p>
     *
     * @return Gets domain name
     */
    @Value.Parameter
    public abstract Optional<String> getDomainName();

    /**
     * <p>
     * getDnsServers.
     * </p>
     *
     * @return Gets DNS servers
     */
    @Value.Parameter
    public abstract DnsServers getDnsServers();

    /**
     * <p>
     * getIpv4DefaultGateway.
     * </p>
     *
     * @return Gets default gateway(routing destination for 0.0.0.0/0) for IPv4
     */
    @Value.Parameter
    public abstract Optional<String> getIpv4DefaultGateway();

    /**
     * <p>
     * getIpv6DefaultGateway.
     * </p>
     *
     * @return Gets default gateway(routing destination for ::/0) for IPv6
     */
    @Value.Parameter
    public abstract Optional<String> getIpv6DefaultGateway();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(HOSTNAME, getHostName());
        visitor.maybeVisit(DOMAINNAME, getDomainName());
        visitor.visitProperties(DNS, getDnsServers());
        visitor.maybeVisit(GATEWAY_IPV_4, getIpv4DefaultGateway());
        visitor.maybeVisit(GATEWAY_IPV_6, getIpv6DefaultGateway());
    }

    public static NetworkOshi from(NetworkParams network) {
        return ImmutableNetworkOshi.builder()
            .hostName(network.getHostName())
            .domainName(network.getDomainName().isEmpty() ? Optional.empty()
                : Optional.of(network.getDomainName()))
            .dnsServers(DnsServers.of(Arrays.asList(network.getDnsServers())))
            .ipv4DefaultGateway(network.getIpv4DefaultGateway().isEmpty() ? Optional.empty()
                : Optional.of(network.getIpv4DefaultGateway()))
            .ipv6DefaultGateway(network.getIpv6DefaultGateway().isEmpty() ? Optional.empty()
                : Optional.of(network.getIpv6DefaultGateway()))
            .build();
    }
}
