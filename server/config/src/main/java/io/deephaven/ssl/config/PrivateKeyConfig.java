package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * The identity material configuration from a certificate and private key.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutablePrivateKeyConfig.class)
public abstract class PrivateKeyConfig implements IdentityConfig {

    public static Builder builder() {
        return ImmutablePrivateKeyConfig.builder();
    }

    /**
     * The certificate chain path.
     */
    public abstract String certChainPath();

    /**
     * The private key path.
     */
    public abstract String privateKeyPath();

    /**
     * The optional private key password.
     */
    public abstract Optional<String> privateKeyPassword();

    /**
     * The optional alias.
     */
    public abstract Optional<String> alias();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }

    public interface Builder {
        Builder certChainPath(String certChainPath);

        Builder privateKeyPath(String privateKeyPath);

        Builder privateKeyPassword(String privateKeyPassword);

        Builder alias(String alias);

        PrivateKeyConfig build();
    }
}
