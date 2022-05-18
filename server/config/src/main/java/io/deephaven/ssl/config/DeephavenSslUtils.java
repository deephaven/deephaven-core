package io.deephaven.ssl.config;

import io.grpc.util.CertificateUtils;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericKeyStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

/**
 * Package private - nl.altindag.ssl is an implementation detail.
 */
class DeephavenSslUtils {
    static SSLFactory create(SSLConfig config) {
        final SSLFactory.Builder builder = SSLFactory.builder();
        // Identity
        {
            if (config.identityProperties()) {
                builder.withSystemPropertyDerivedIdentityMaterial();
            }
            for (IdentityConfig identity : config.identity()) {
                addIdentity(builder, identity);
            }
        }
        // Trust
        {
            if (config.trustAll()) {
                builder.withTrustingAllCertificatesWithoutValidation();
            } else {
                if (config.trustJdk()) {
                    builder.withDefaultTrustMaterial();
                }
                if (config.trustSystem()) {
                    builder.withSystemTrustMaterial();
                }
                if (config.trustProperties()) {
                    builder.withSystemPropertyDerivedTrustMaterial();
                }
                for (TrustConfig trust : config.trust()) {
                    addTrust(builder, trust);
                }
            }
        }
        // Ciphers
        {
            if (config.ciphersProperties()) {
                builder.withSystemPropertyDerivedCiphers();
            }
            builder.withCiphers(config.ciphers().toArray(new String[0]));
        }
        // Protocols
        {
            if (config.protocolsProperties()) {
                builder.withSystemPropertyDerivedProtocols();
            }
            builder.withProtocols(config.protocols().toArray(new String[0]));
        }
        // Client authentication
        switch (config.clientAuthentication()) {
            case WANTED:
                builder.withWantClientAuthentication();
                break;
            case NEEDED:
                builder.withNeedClientAuthentication();
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected client auth: " + config.clientAuthentication());
        }
        return builder.build();
    }

    private static void addTrust(SSLFactory.Builder builder, TrustConfig config) {
        config.walk(new TrustConfig.Visitor<Void>() {
            @Override
            public Void visit(TrustStoreConfig trustStore) {
                addTrust(builder, trustStore);
                return null;
            }

            @Override
            public Void visit(TrustCertificatesConfig certificates) {
                addTrust(builder, certificates);
                return null;
            }
        });
    }

    private static void addTrust(SSLFactory.Builder builder, TrustCertificatesConfig config) {
        for (String path : config.path()) {
            try {
                final X509Certificate[] x509Certificates = readX509Certificates(Paths.get(path));
                builder.withTrustMaterial(x509Certificates);
            } catch (GenericKeyStoreException e) {
                throw new RuntimeException(e.getCause());
            } catch (CertificateException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static void addTrust(SSLFactory.Builder builder, TrustStoreConfig config) {
        try {
            final char[] password = config.password().toCharArray();
            builder.withTrustMaterial(Paths.get(config.path()), password);
        } catch (GenericKeyStoreException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static void addIdentity(SSLFactory.Builder builder, IdentityConfig config) {
        config.walk(new IdentityConfig.Visitor<Void>() {
            @Override
            public Void visit(KeyStoreConfig keyStore) {
                addIdentity(builder, keyStore);
                return null;
            }

            @Override
            public Void visit(PrivateKeyConfig privateKey) {
                addIdentity(builder, privateKey);
                return null;
            }
        });
    }

    private static void addIdentity(SSLFactory.Builder builder, KeyStoreConfig config) {
        final char[] password = config.password().toCharArray();
        try {
            if (config.keystoreType().isPresent()) {
                builder.withIdentityMaterial(Paths.get(config.path()), password, config.keystoreType().get());
            } else {
                builder.withIdentityMaterial(Paths.get(config.path()), password);
            }
        } catch (GenericKeyStoreException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static void addIdentity(SSLFactory.Builder builder, PrivateKeyConfig config) {
        try {
            final PrivateKey privateKey = readPrivateKey(Paths.get(config.privateKeyPath()));
            final X509Certificate[] x509Certificates = readX509Certificates(Paths.get(config.certChainPath()));
            final char[] password = config.privateKeyPassword().map(String::toCharArray).orElse(null);
            final String alias = config.alias().orElse(null);
            builder.withIdentityMaterial(privateKey, password, alias, x509Certificates);
        } catch (GenericKeyStoreException e) {
            throw new RuntimeException(e.getCause());
        } catch (CertificateException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static PrivateKey readPrivateKey(Path path)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (final InputStream in = Files.newInputStream(path)) {
            return CertificateUtils.getPrivateKey(in);
        }
    }

    private static X509Certificate[] readX509Certificates(Path path) throws IOException, CertificateException {
        try (final InputStream in = Files.newInputStream(path)) {
            return CertificateUtils.getX509Certificates(in);
        }
    }
}
