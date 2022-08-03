/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The trust material configuration.
 *
 * @see TrustStore
 * @see TrustCertificates
 * @see TrustJdk
 * @see TrustProperties
 * @see TrustSystem
 * @see TrustAll
 * @see TrustList
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TrustStore.class, name = "truststore"),
        @JsonSubTypes.Type(value = TrustCertificates.class, name = "certs"),
        @JsonSubTypes.Type(value = TrustJdk.class, name = "jdk"),
        @JsonSubTypes.Type(value = TrustProperties.class, name = "properties"),
        @JsonSubTypes.Type(value = TrustSystem.class, name = "system"),
        @JsonSubTypes.Type(value = TrustAll.class, name = "all"),
        @JsonSubTypes.Type(value = TrustList.class, name = "list"),
})
public interface Trust {
    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(TrustStore trustStore);

        T visit(TrustCertificates certificates);

        T visit(TrustJdk jdk);

        T visit(TrustProperties properties);

        T visit(TrustSystem system);

        T visit(TrustAll all);

        T visit(TrustList list);
    }
}
