package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The identity material configuration.
 *
 * @see KeyStoreConfig
 * @see PrivateKeyConfig
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KeyStoreConfig.class, name = "keystore"),
        @JsonSubTypes.Type(value = PrivateKeyConfig.class, name = "privatekey"),
})
public interface IdentityConfig {
    <V extends Visitor<T>, T> T walk(V visitor);

    interface Visitor<T> {
        T visit(KeyStoreConfig keyStore);

        T visit(PrivateKeyConfig privateKey);
    }
}
