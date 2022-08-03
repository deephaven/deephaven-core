/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The identity material configuration.
 *
 * @see IdentityKeyStore
 * @see IdentityPrivateKey
 * @see IdentityProperties
 * @see IdentityList
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = IdentityKeyStore.class, name = "keystore"),
        @JsonSubTypes.Type(value = IdentityPrivateKey.class, name = "privatekey"),
        @JsonSubTypes.Type(value = IdentityProperties.class, name = "properties"),
        @JsonSubTypes.Type(value = IdentityList.class, name = "list"),

})
public interface Identity {
    <V extends Visitor<T>, T> T walk(V visitor);

    interface Visitor<T> {
        T visit(IdentityKeyStore keyStore);

        T visit(IdentityPrivateKey privateKey);

        T visit(IdentityProperties properties);

        T visit(IdentityList list);
    }
}
