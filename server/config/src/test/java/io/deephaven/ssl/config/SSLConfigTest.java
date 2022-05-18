package io.deephaven.ssl.config;

import io.deephaven.ssl.config.SSLConfig.ClientAuth;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

public class SSLConfigTest {

    @Test
    void defaultExplicit() throws IOException {
        check("default-explicit.json", SSLConfig.defaultConfig());
    }

    @Test
    void defaultImplicit() throws IOException {
        check("default-implicit.json", SSLConfig.defaultConfig());
    }

    @Test
    void identityKeystore() throws IOException {
        check("identity-keystore.json",
                SSLConfig.builder().addIdentity(KeyStoreConfig.of("path.p12", "secret")).build());
    }

    @Test
    void identityPrivatekey() throws IOException {
        check("identity-privatekey.json",
                SSLConfig.builder()
                        .addIdentity(
                                PrivateKeyConfig.builder().certChainPath("ca.crt").privateKeyPath("key.pem").build())
                        .build());
    }

    @Test
    void identityMultiple() throws IOException {
        check("identity-multiple.json",
                SSLConfig.builder().addIdentity(PrivateKeyConfig.builder().certChainPath("ca.crt")
                        .privateKeyPath("key.pem").alias("alias-1").build(), KeyStoreConfig.of("path.p12", "secret"))
                        .build());
    }


    @Test
    void identityProperties() throws IOException {
        check("identity-properties.json", SSLConfig.builder().identityProperties(true).build());
    }

    @Test
    void trustCertificates() throws IOException {
        check("trust-certificates.json", SSLConfig.builder().addTrust(TrustCertificatesConfig.of("ca.crt")).build());
    }

    @Test
    void trustStore() throws IOException {
        check("trust-store.json", SSLConfig.builder().addTrust(TrustStoreConfig.of("ca.p12", "secret")).build());
    }

    @Test
    void trustMultiple() throws IOException {
        check("trust-multiple.json", SSLConfig.builder()
                .addTrust(TrustCertificatesConfig.of("ca-1.crt", "ca-2.crt"), TrustStoreConfig.of("ca-3.p12", "secret"))
                .build());
    }

    @Test
    void trustJdk() throws IOException {
        check("trust-jdk.json", SSLConfig.builder().trustJdk(false).build());
    }

    @Test
    void trustSystem() throws IOException {
        check("trust-system.json", SSLConfig.builder().trustSystem(true).build());
    }

    @Test
    void trustProperties() throws IOException {
        check("trust-properties.json", SSLConfig.builder().trustProperties(true).build());
    }

    @Test
    void trustAll() throws IOException {
        check("trust-all.json", SSLConfig.builder().trustAll(true).build());
    }

    @Test
    void ciphers() throws IOException {
        check("ciphers.json", SSLConfig.builder().addCiphers("TLS_AES_128_GCM_SHA256").build());
    }

    @Test
    void ciphersProperties() throws IOException {
        check("ciphers-properties.json", SSLConfig.builder().ciphersProperties(true).build());
    }

    @Test
    void protocols() throws IOException {
        check("protocols.json", SSLConfig.builder().addProtocols("TLSv1.3").build());
    }

    @Test
    void protocolsProperties() throws IOException {
        check("protocols-properties.json", SSLConfig.builder().protocolsProperties(true).build());
    }

    @Test
    void clientAuthenticationWanted() throws IOException {
        check("client-authentication-wanted.json", SSLConfig.builder().clientAuthentication(ClientAuth.WANTED).build());
    }

    @Test
    void clientAuthenticationNeeded() throws IOException {
        check("client-authentication-needed.json", SSLConfig.builder().clientAuthentication(ClientAuth.NEEDED).build());
    }

    private static void check(String name, SSLConfig expected) throws IOException {
        assertThat(sslConfig(name)).isEqualTo(expected);
    }

    private static SSLConfig sslConfig(String name) throws IOException {
        return SSLConfig.parseJson(resource(name));
    }

    private static URL resource(String name) {
        return SSLConfigTest.class.getResource(name);
    }
}
