/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The TLS ciphers.
 *
 * @see CiphersJdk
 * @see CiphersModern
 * @see CiphersProperties
 * @see CiphersExplicit
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CiphersJdk.class, name = "jdk"),
        @JsonSubTypes.Type(value = CiphersModern.class, name = "modern"),
        @JsonSubTypes.Type(value = CiphersIntermediate.class, name = "intermediate"),
        @JsonSubTypes.Type(value = CiphersProperties.class, name = "properties"),
        @JsonSubTypes.Type(value = CiphersExplicit.class, name = "explicit"),
})
public interface Ciphers {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {

        T visit(CiphersJdk jdk);

        T visit(CiphersModern modern);

        T visit(CiphersIntermediate intermediate);

        T visit(CiphersProperties properties);

        T visit(CiphersExplicit explicit);
    }
}
