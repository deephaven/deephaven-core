/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import io.deephaven.ssl.config.SSLConfig.ClientAuth;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class SSLConfigTest {

    @Test
    void defaultExplicit() throws IOException {
        check("default-explicit.json", SSLConfig.builder().build());
    }

    @Test
    void defaultImplicit() throws IOException {
        check("default-implicit.json", SSLConfig.builder().build());
    }

    @Test
    void identityKeystore() throws IOException {
        check("identity-keystore.json", server(IdentityKeyStore.of("path.p12", "secret")));
    }

    @Test
    void identityPrivatekey() throws IOException {
        check("identity-privatekey.json",
                server(IdentityPrivateKey.builder().certChainPath("ca.crt").privateKeyPath("key.pem").build()));
    }

    @Test
    void identityMultiple() throws IOException {
        check("identity-multiple.json", server(IdentityList.of(
                IdentityPrivateKey.builder().certChainPath("ca.crt").privateKeyPath("key.pem").alias("alias-1").build(),
                IdentityKeyStore.of("path.p12", "secret"))));
    }

    @Test
    void identityProperties() throws IOException {
        check("identity-properties.json", server(IdentityProperties.of()));
    }

    @Test
    void trustCertificates() throws IOException {
        check("trust-certificates.json", SSLConfig.builder().trust(TrustCertificates.of("ca.crt")).build());
    }

    @Test
    void trustStore() throws IOException {
        check("trust-store.json", SSLConfig.builder().trust(TrustStore.of("ca.p12", "secret")).build());
    }

    @Test
    void trustMultiple() throws IOException {
        check("trust-multiple.json", SSLConfig.builder()
                .trust(TrustList.of(
                        TrustCertificates.of("ca-1.crt", "ca-2.crt"),
                        TrustStore.of("ca-3.p12", "secret")))
                .build());
    }

    @Test
    void trustJdk() throws IOException {
        check("trust-jdk.json", SSLConfig.builder().trust(TrustJdk.of()).build());
    }

    @Test
    void trustSystem() throws IOException {
        check("trust-system.json", SSLConfig.builder().trust(TrustSystem.of()).build());
    }

    @Test
    void trustProperties() throws IOException {
        check("trust-properties.json", SSLConfig.builder().trust(TrustProperties.of()).build());
    }

    @Test
    void trustAll() throws IOException {
        check("trust-all.json", SSLConfig.builder().trust(TrustAll.of()).build());
    }

    @Test
    void ciphersExplicit() throws IOException {
        check("ciphers-explicit.json",
                SSLConfig.builder().ciphers(CiphersExplicit.of("TLS_AES_128_GCM_SHA256")).build());
    }

    @Test
    void ciphersProperties() throws IOException {
        check("ciphers-properties.json", SSLConfig.builder().ciphers(CiphersProperties.of()).build());
    }

    @Test
    void ciphersModern() throws IOException {
        check("ciphers-modern.json", SSLConfig.builder().ciphers(CiphersModern.of()).build());
    }

    @Test
    void ciphersIntermediate() throws IOException {
        check("ciphers-intermediate.json", SSLConfig.builder().ciphers(CiphersIntermediate.of()).build());
    }

    @Test
    void ciphersJdk() throws IOException {
        check("ciphers-jdk.json", SSLConfig.builder().ciphers(CiphersJdk.of()).build());
    }

    @Test
    void protocolsExplicit() throws IOException {
        check("protocols-explicit.json", SSLConfig.builder().protocols(ProtocolsExplicit.of("TLSv1.3")).build());
    }

    @Test
    void protocolsProperties() throws IOException {
        check("protocols-properties.json", SSLConfig.builder().protocols(ProtocolsProperties.of()).build());
    }

    @Test
    void protocolsModern() throws IOException {
        check("protocols-modern.json", SSLConfig.builder().protocols(ProtocolsModern.of()).build());
    }

    @Test
    void protocolsIntermediate() throws IOException {
        check("protocols-intermediate.json", SSLConfig.builder().protocols(ProtocolsIntermediate.of()).build());
    }

    @Test
    void protocolsJdk() throws IOException {
        check("protocols-jdk.json", SSLConfig.builder().protocols(ProtocolsJdk.of()).build());
    }

    @Test
    void clientAuthenticationWanted() throws IOException {
        check("client-authentication-wanted.json", SSLConfig.builder()
                .clientAuthentication(ClientAuth.WANTED)
                .trust(TrustJdk.of())
                .identity(IdentityProperties.of())
                .build());
    }

    @Test
    void clientAuthenticationNeeded() throws IOException {
        check("client-authentication-needed.json", SSLConfig.builder()
                .clientAuthentication(ClientAuth.NEEDED)
                .trust(TrustJdk.of())
                .identity(IdentityProperties.of())
                .build());
    }

    private static SSLConfig server(Identity identity) {
        return SSLConfig.builder().identity(identity).build();
    }

    private static void check(String name, SSLConfig expected) throws IOException {
        assertThat(sslConfig(name)).isEqualTo(expected);
    }

    private static void checkIllegal(String name) throws IOException {
        try {
            sslConfig(name);
            failBecauseExceptionWasNotThrown(ValueInstantiationException.class);
        } catch (ValueInstantiationException e) {
            // expected
        }
    }

    private static SSLConfig sslConfig(String name) throws IOException {
        return SSLConfig.parseJson(resource(name));
    }

    private static URL resource(String name) {
        return SSLConfigTest.class.getResource(name);
    }
}
