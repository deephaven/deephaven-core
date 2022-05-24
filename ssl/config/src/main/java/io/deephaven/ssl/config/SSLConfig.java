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
@Immutable(copy = true)
@BuildableStyle
@JsonDeserialize(as = ImmutableSSLConfig.class)
public abstract class SSLConfig {

    public static Builder builder() {
        return ImmutableSSLConfig.builder();
    }

    /**
     * An empty SSL configuration, equivalent to {@code builder().build()}.
     *
     * @return the empty configuration
     */
    public static SSLConfig empty() {
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

    public final SSLConfig orTrust(Trust defaultTrust) {
        if (trust().isPresent()) {
            return this;
        }
        return ((ImmutableSSLConfig) this).withTrust(defaultTrust);
    }

    public final SSLConfig orProtocols(Protocols defaultProtocols) {
        if (protocols().isPresent()) {
            return this;
        }
        return ((ImmutableSSLConfig) this).withProtocols(defaultProtocols);
    }

    public final SSLConfig orCiphers(Ciphers defaultCiphers) {
        if (ciphers().isPresent()) {
            return this;
        }
        return ((ImmutableSSLConfig) this).withCiphers(defaultCiphers);
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
