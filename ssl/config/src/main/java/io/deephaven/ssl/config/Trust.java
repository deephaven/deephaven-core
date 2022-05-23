package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The trust material configuration.
 * 
 * @see TrustStoreConfig
 * @see TrustCertificatesConfig
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TrustStoreConfig.class, name = "truststore"),
        @JsonSubTypes.Type(value = TrustCertificatesConfig.class, name = "certs"),
        @JsonSubTypes.Type(value = TrustJdk.class, name = "jdk"),
        @JsonSubTypes.Type(value = TrustProperties.class, name = "properties"),
        @JsonSubTypes.Type(value = TrustSystem.class, name = "system"),
        @JsonSubTypes.Type(value = TrustAll.class, name = "all"),
        @JsonSubTypes.Type(value = TrustList.class, name = "list"),
})
public interface Trust {
    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(TrustStoreConfig trustStore);

        T visit(TrustCertificatesConfig certificates);

        T visit(TrustJdk jdk);

        T visit(TrustProperties properties);

        T visit(TrustSystem system);

        T visit(TrustAll all);

        T visit(TrustList list);
    }
}
