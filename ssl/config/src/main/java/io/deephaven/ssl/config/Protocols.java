//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The TLS protocols.
 *
 * @see ProtocolsJdk
 * @see ProtocolsModern
 * @see ProtocolsProperties
 * @see ProtocolsExplicit
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ProtocolsJdk.class, name = "jdk"),
        @JsonSubTypes.Type(value = ProtocolsModern.class, name = "modern"),
        @JsonSubTypes.Type(value = ProtocolsIntermediate.class, name = "intermediate"),
        @JsonSubTypes.Type(value = ProtocolsProperties.class, name = "properties"),
        @JsonSubTypes.Type(value = ProtocolsExplicit.class, name = "explicit"),
})
public interface Protocols {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ProtocolsJdk jdk);

        T visit(ProtocolsModern modern);

        T visit(ProtocolsIntermediate intermediate);

        T visit(ProtocolsProperties properties);

        T visit(ProtocolsExplicit explicit);
    }
}
