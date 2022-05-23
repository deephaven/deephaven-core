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

    public static Builder builder() {
        return ImmutableSSLConfig.builder();
    }

    /**
     * The default configuration includes JDK default where applicable. This configuration is suitable for generic
     * clients connecting to public services. The default configuration is represented by the explicit JSON:
     * 
     * <pre>
     * {
     *   "identity": null,
     *   "trust": {
     *     "type": "jdk"
     *   },
     *   "ciphers": {
     *     "type": "jdk"
     *   },
     *   "protocols": {
     *     "type": "jdk"
     *   },
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
     * The identity material. Optional for clients, necessary for servers.
     */
    public abstract Optional<Identity> identity();

    /**
     * The trust material. Defaults to {@link TrustJdk#of() jdk}.
     */
    @Default
    public Trust trust() {
        return TrustJdk.of();
    }

    /**
     * The protocols. Defaults to {@link ProtocolsJdk#of() jdk}.
     *
     * @return the protocols
     */
    @Default
    public Protocols protocols() {
        return ProtocolsJdk.of();
    }

    /**
     * The ciphers. Defaults to {@link CiphersJdk#of() jdk}.
     *
     * @return the ciphers
     */
    @Default
    public Ciphers ciphers() {
        return CiphersJdk.of();
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
        Builder identity(Identity identity);

        Builder trust(Trust trust);

        Builder ciphers(Ciphers ciphers);

        Builder protocols(Protocols protocols);

        Builder clientAuthentication(ClientAuth clientAuthentication);

        SSLConfig build();
    }
}
