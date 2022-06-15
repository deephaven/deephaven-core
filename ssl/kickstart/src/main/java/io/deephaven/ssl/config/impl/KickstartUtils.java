/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config.impl;

import io.deephaven.ssl.config.Ciphers;
import io.deephaven.ssl.config.CiphersExplicit;
import io.deephaven.ssl.config.CiphersIntermediate;
import io.deephaven.ssl.config.CiphersJdk;
import io.deephaven.ssl.config.CiphersModern;
import io.deephaven.ssl.config.CiphersProperties;
import io.deephaven.ssl.config.Identity;
import io.deephaven.ssl.config.IdentityKeyStore;
import io.deephaven.ssl.config.IdentityList;
import io.deephaven.ssl.config.IdentityPrivateKey;
import io.deephaven.ssl.config.IdentityProperties;
import io.deephaven.ssl.config.Protocols;
import io.deephaven.ssl.config.ProtocolsExplicit;
import io.deephaven.ssl.config.ProtocolsIntermediate;
import io.deephaven.ssl.config.ProtocolsJdk;
import io.deephaven.ssl.config.ProtocolsModern;
import io.deephaven.ssl.config.ProtocolsProperties;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.Trust;
import io.deephaven.ssl.config.TrustAll;
import io.deephaven.ssl.config.TrustCertificates;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.TrustList;
import io.deephaven.ssl.config.TrustProperties;
import io.deephaven.ssl.config.TrustStore;
import io.deephaven.ssl.config.TrustSystem;
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

public class KickstartUtils {

    public static SSLFactory create(SSLConfig config) {
        final SSLFactory.Builder builder = SSLFactory.builder();
        config.identity().ifPresent(identity -> addIdentity(builder, identity));
        config.trust().ifPresent(trust -> addTrust(builder, trust));
        config.protocols().ifPresent(protocols -> addProtocols(builder, protocols));
        config.ciphers().ifPresent(ciphers -> addCiphers(builder, ciphers));
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

    private static void addTrust(SSLFactory.Builder builder, Trust config) {
        config.walk(new Trust.Visitor<Void>() {

            @Override
            public Void visit(TrustStore trustStore) {
                addTrust(builder, trustStore);
                return null;
            }

            @Override
            public Void visit(TrustCertificates certificates) {
                addTrust(builder, certificates);
                return null;
            }

            @Override
            public Void visit(TrustJdk jdk) {
                builder.withDefaultTrustMaterial();
                return null;
            }

            @Override
            public Void visit(TrustProperties properties) {
                builder.withSystemPropertyDerivedTrustMaterial();
                return null;
            }

            @Override
            public Void visit(TrustSystem system) {
                builder.withSystemTrustMaterial();
                return null;
            }

            @Override
            public Void visit(TrustAll all) {
                builder.withTrustingAllCertificatesWithoutValidation();
                return null;
            }

            @Override
            public Void visit(TrustList list) {
                for (Trust trust : list.values()) {
                    addTrust(builder, trust);
                }
                return null;
            }
        });
    }

    private static void addTrust(SSLFactory.Builder builder, TrustCertificates config) {
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

    private static void addTrust(SSLFactory.Builder builder, TrustStore config) {
        try {
            final char[] password = config.password().toCharArray();
            builder.withTrustMaterial(Paths.get(config.path()), password);
        } catch (GenericKeyStoreException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static void addIdentity(SSLFactory.Builder builder, Identity config) {
        config.walk(new Identity.Visitor<Void>() {
            @Override
            public Void visit(IdentityKeyStore keyStore) {
                addIdentity(builder, keyStore);
                return null;
            }

            @Override
            public Void visit(IdentityPrivateKey privateKey) {
                addIdentity(builder, privateKey);
                return null;
            }

            @Override
            public Void visit(IdentityProperties properties) {
                builder.withSystemPropertyDerivedIdentityMaterial();
                return null;
            }

            @Override
            public Void visit(IdentityList list) {
                for (Identity value : list.values()) {
                    addIdentity(builder, value);
                }
                return null;
            }
        });
    }

    private static void addCiphers(SSLFactory.Builder builder, Ciphers ciphers) {
        ciphers.walk(new Ciphers.Visitor<Void>() {
            @Override
            public Void visit(CiphersJdk jdk) {
                // no-op, kickstart will default to JDK
                return null;
            }

            @Override
            public Void visit(CiphersModern modern) {
                builder.withCiphers(modern.ciphers().toArray(new String[0]));
                return null;
            }

            @Override
            public Void visit(CiphersIntermediate intermediate) {
                builder.withCiphers(intermediate.ciphers().toArray(new String[0]));
                return null;
            }

            @Override
            public Void visit(CiphersProperties properties) {
                builder.withSystemPropertyDerivedCiphers();
                return null;
            }

            @Override
            public Void visit(CiphersExplicit explicit) {
                builder.withCiphers(explicit.values().toArray(new String[0]));
                return null;
            }
        });
    }

    private static void addProtocols(SSLFactory.Builder builder, Protocols protocols) {
        protocols.walk(new Protocols.Visitor<Object>() {
            @Override
            public Object visit(ProtocolsJdk jdk) {
                // no-op, kickstart will default to JDK
                return null;
            }

            @Override
            public Object visit(ProtocolsModern modern) {
                builder.withProtocols(modern.protocols().toArray(new String[0]));
                return null;
            }

            @Override
            public Object visit(ProtocolsIntermediate intermediate) {
                builder.withProtocols(intermediate.protocols().toArray(new String[0]));
                return null;
            }

            @Override
            public Object visit(ProtocolsProperties properties) {
                builder.withSystemPropertyDerivedProtocols();
                return null;
            }

            @Override
            public Object visit(ProtocolsExplicit explicit) {
                builder.withProtocols(explicit.values().toArray(new String[0]));
                return null;
            }
        });
    }

    private static void addIdentity(SSLFactory.Builder builder, IdentityKeyStore config) {
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

    private static void addIdentity(SSLFactory.Builder builder, IdentityPrivateKey config) {
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
