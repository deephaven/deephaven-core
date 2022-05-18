package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

/**
 * The SSL configuration object.
 *
 * @see io.deephaven.ssl.config
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {

    public static Builder builder() {
        return ImmutableSSLConfig.builder();
    }

    /**
     * The default configuration includes {@link #trustJdk()}, but no other trust nor identity material. This
     * configuration is suitable for generic clients connecting to public services. The default configuration is
     * represented by the JSON:
     * 
     * <pre>
     * {
     *   "identity": [],
     *   "identityProperties": false,
     *
     *   "trust": [],
     *   "trustJdk": true,
     *   "trustSystem": false,
     *   "trustProperties": false,
     *   "trustAll": false,
     *
     *   "ciphers": [],
     *   "ciphersProperties": false,
     *
     *   "protocols": [],
     *   "protocolsProperties": false,
     *
     *   "clientAuthentication": "NONE"
     * }
     * </pre>
     *
     * @return the default configuration
     */
    public static SSLConfig defaultConfig() {
        return builder().build();
    }

    public static SSLConfig parseJson(Path path) throws IOException {
        return Parser.parseJson(path.toFile(), SSLConfig.class);
    }

    public static SSLConfig parseJson(String value) throws IOException {
        return Parser.parseJson(value, SSLConfig.class);
    }

    public static SSLConfig parseJson(URL url) throws IOException {
        return Parser.parseJson(url, SSLConfig.class);
    }

    /**
     * The identity material.
     */
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<IdentityConfig> identity();

    /**
     * Include the identity material defined by the system properties "javax.net.ssl.keyStore",
     * "javax.net.ssl.keyStorePassword", "javax.net.ssl.keyStoreType", and "javax.net.ssl.keyStoreProvider". Defaults to
     * {@code false}.
     */
    @Default
    public boolean identityProperties() {
        return false;
    }

    /**
     * The trust material.
     */
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<TrustConfig> trust();

    /**
     * Include the default JDK trust material. Defaults to {@code true}.
     */
    @Default
    public boolean trustJdk() {
        return true;
    }

    /**
     * Include the trust material defined by the operating system. Only Mac and Windows at the moment. Defaults to
     * {@code false}.
     */
    @Default
    public boolean trustSystem() {
        return false;
    }

    /**
     * Include the trust material defined by the system properties "javax.net.ssl.trustStore",
     * "javax.net.ssl.trustStorePassword", "javax.net.ssl.trustStoreType", and "javax.net.ssl.trustStoreProvider".
     * Defaults to {@code false}.
     */
    @Default
    public boolean trustProperties() {
        return false;
    }

    /**
     * Trust all certificates without validation. Should not be used in production. Defaults to {@code false}.
     */
    @Default
    public boolean trustAll() {
        return false;
    }

    /**
     * The ciphers.
     */
    public abstract List<String> ciphers();

    /**
     * Include the ciphers defined by the system properties "https.cipherSuites". Defaults to {@code false}.
     */
    @Default
    public boolean ciphersProperties() {
        return false;
    }

    /**
     * The protocols.
     */
    public abstract List<String> protocols();

    /**
     * Include the protocols defined by the system properties "https.protocols". Defaults to {@code false}.
     */
    @Default
    public boolean protocolsProperties() {
        return false;
    }

    /**
     * Client authentication. Defaults to {@link ClientAuth#NONE NONE}.
     */
    @Default
    public ClientAuth clientAuthentication() {
        return ClientAuth.NONE;
    }

    public enum ClientAuth {
        NONE, WANTED, NEEDED
    }

    public interface Builder {
        Builder addIdentity(IdentityConfig element);

        Builder addIdentity(IdentityConfig... elements);

        Builder addAllIdentity(Iterable<? extends IdentityConfig> elements);

        Builder identityProperties(boolean identityProperties);

        Builder addTrust(TrustConfig element);

        Builder addTrust(TrustConfig... elements);

        Builder addAllTrust(Iterable<? extends TrustConfig> elements);

        Builder trustJdk(boolean trustJdk);

        Builder trustSystem(boolean trustSystem);

        Builder trustProperties(boolean trustProperties);

        Builder trustAll(boolean trustAll);

        Builder addCiphers(String element);

        Builder addCiphers(String... elements);

        Builder addAllCiphers(Iterable<String> elements);

        Builder ciphersProperties(boolean ciphersProperties);

        Builder addProtocols(String element);

        Builder addProtocols(String... elements);

        Builder addAllProtocols(Iterable<String> elements);

        Builder protocolsProperties(boolean protocolsProperties);

        Builder clientAuthentication(ClientAuth clientAuthentication);

        SSLConfig build();
    }
}
