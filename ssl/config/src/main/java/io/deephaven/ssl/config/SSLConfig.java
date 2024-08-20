//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.CopyableStyle;
import org.immutables.value.Value;
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
@CopyableStyle
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
     * The optional client authentication.
     */
    public abstract Optional<ClientAuth> clientAuthentication();

    public abstract SSLConfig withTrust(Trust trust);

    public abstract SSLConfig withProtocols(Protocols protocols);

    public abstract SSLConfig withCiphers(Ciphers ciphers);

    public final SSLConfig orTrust(Trust trust) {
        return trust().isPresent() ? this : withTrust(trust);
    }

    public final SSLConfig orProtocols(Protocols protocols) {
        return protocols().isPresent() ? this : withProtocols(protocols);
    }

    public final SSLConfig orCiphers(Ciphers ciphers) {
        return ciphers().isPresent() ? this : withCiphers(ciphers);
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

    @Value.Check
    final void checkMutualTLS() {
        if (clientAuthentication().orElse(ClientAuth.NONE) != ClientAuth.NONE) {
            if (!trust().isPresent()) {
                throw new IllegalArgumentException("Trust material must be present when requesting mutual TLS");
            }
            if (!identity().isPresent()) {
                throw new IllegalArgumentException("Identity material must be present when requesting mutual TLS");
            }
        }
    }
}
