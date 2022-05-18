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
})
public interface TrustConfig {
    <V extends Visitor<T>, T> T walk(V visitor);

    interface Visitor<T> {
        T visit(TrustStoreConfig trustStore);

        T visit(TrustCertificatesConfig certificates);
    }
}
