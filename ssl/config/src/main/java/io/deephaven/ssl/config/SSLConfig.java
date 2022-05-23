package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The SSL configuration object.
 *
 * @see io.deephaven.ssl.config
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {

    public static final TrustJdk DEFAULT_CLIENT_TRUST = TrustJdk.of();
    public static final ProtocolsModern DEFAULT_CLIENT_PROTOCOLS = ProtocolsModern.of();
    public static final CiphersModern DEFAULT_CLIENT_CIPHERS = CiphersModern.of();

    public static Builder builder() {
        return ImmutableSSLConfig.builder();
    }

    /**
     * The default client configuration is suitable for clients connecting to public Deephaven services. The default
     * configuration is represented by the JSON:
     * 
     * <pre>
     * {
     *   "trust": {
     *     "type": "jdk"
     *   },
     *   "protocols": {
     *     "type": "modern"
     *   },
     *   "ciphers": {
     *     "type": "modern"
     *   }
     * }
     * </pre>
     *
     * @return the default client configuration
     */
    public static SSLConfig defaultClient() {
        return builder()
                .trust(DEFAULT_CLIENT_TRUST)
                .protocols(DEFAULT_CLIENT_PROTOCOLS)
                .ciphers(DEFAULT_CLIENT_CIPHERS)
                .build();
    }

    /**
     * Creates a server configuration with JDK default compatibility.
     */
    public static SSLConfig jdkDefaultServer(Identity identity) {
        return builder()
                .identity(identity)
                .protocols(ProtocolsJdk.of())
                .ciphers(CiphersJdk.of())
                .build();
    }

    /**
     * Creates a server configuration with modern compatibility.
     *
     * @see ProtocolsModern
     * @see CiphersModern
     */
    public static SSLConfig modernCompatibilityServer(Identity identity) {
        return builder()
                .identity(identity)
                .protocols(ProtocolsModern.of())
                .ciphers(CiphersModern.of())
                .build();
    }

    /**
     * Creates a server configuration with intermediate compatibility.
     *
     * @see ProtocolsIntermediate
     * @see CiphersIntermediate
     */
    public static SSLConfig intermediateCompatibilityServer(Identity identity) {
        return builder()
                .identity(identity)
                .protocols(ProtocolsIntermediate.of())
                .ciphers(CiphersIntermediate.of())
                .build();
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
     * The identity material. Optional for clients, necessary for servers.
     */
    public abstract Optional<Identity> identity();

    /**
     * The optional trust material.
     */
    public abstract Optional<Trust> trust();

    /**
     * The optional protocols.
     */
    public abstract Optional<Protocols> protocols();

    /**
     * The optional ciphers.
     */
    public abstract Optional<Ciphers> ciphers();

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
        Builder identity(Identity identity);

        Builder trust(Trust trust);

        Builder protocols(Protocols protocols);

        Builder ciphers(Ciphers ciphers);

        Builder clientAuthentication(ClientAuth clientAuthentication);

        SSLConfig build();
    }
}
