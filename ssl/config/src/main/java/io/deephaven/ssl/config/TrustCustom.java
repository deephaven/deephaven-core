//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.List;

/**
 * A custom trust configuration provided via {@link Certificate}. This is not currently deserializable via JSON.
 */
@Immutable
@BuildableStyle
public abstract class TrustCustom extends TrustBase {
    private static final String X_509 = "X.509";

    public static Builder builder() {
        return ImmutableTrustCustom.builder();
    }

    /**
     * Creates a trust from the given {@code factory} and {@code in}. Equivalent to
     * {@code builder().addAllCertificates(factory.generateCertificates(in)).build()}.
     *
     * @param factory the certificate factory
     * @param in the input stream
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     */
    public static TrustCustom of(CertificateFactory factory, InputStream in) throws IOException, CertificateException {
        return builder().addAllCertificates(factory.generateCertificates(in)).build();
    }

    /**
     * Creates a trust from the given {@code factory} and {@code path}.
     *
     * @param factory the certificate factory
     * @param path the path
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     */
    public static TrustCustom of(CertificateFactory factory, Path path) throws IOException, CertificateException {
        try (final InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
            return of(factory, in);
        }
    }

    /**
     * Creates a trust from the given {@code factory} and {@code in}. Equivalent to
     * {@code of(factory, new ByteArrayInputStream(in, offset, length))}.
     *
     * @param factory the certificate factory
     * @param in the input bytes
     * @param offset the input offset
     * @param length the input length
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     * @see ByteArrayInputStream#ByteArrayInputStream(byte[], int, int)
     * @see #of(CertificateFactory, InputStream).
     */
    public static TrustCustom of(CertificateFactory factory, byte[] in, int offset, int length)
            throws IOException, CertificateException {
        return of(factory, new ByteArrayInputStream(in, offset, length));
    }

    /**
     * Creates an X509 trust from the given {@code in}. Equivalent to
     * {@code of(CertificateFactory.getInstance("X.509"), in)}.
     *
     * @param in the input stream
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     * @see CertificateFactory#getInstance(String)
     * @see #of(CertificateFactory, InputStream)
     */
    public static TrustCustom ofX509(InputStream in) throws CertificateException, IOException {
        return of(CertificateFactory.getInstance(X_509), in);
    }

    /**
     * Creates an X509 trust from the given {@code path}. Equivalent to
     * {@code of(CertificateFactory.getInstance("X.509"), path)}.
     *
     * @param path the path
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     * @see CertificateFactory#getInstance(String)
     * @see #of(CertificateFactory, Path)
     */
    public static TrustCustom ofX509(Path path) throws CertificateException, IOException {
        return of(CertificateFactory.getInstance(X_509), path);
    }

    /**
     * Creates an X509 trust from the given {@code in}. Equivalent to
     * {@code of(CertificateFactory.getInstance("X.509"), in, offset, length)}.
     *
     * @param in the input bytes
     * @param offset the input offset
     * @param length the input length
     * @return the trust
     * @throws IOException if an IO exception occurs
     * @throws CertificateException on parsing errors
     * @see CertificateFactory#getInstance(String)
     * @see #of(CertificateFactory, byte[], int, int)
     */
    public static TrustCustom ofX509(byte[] in, int offset, int length) throws CertificateException, IOException {
        return of(CertificateFactory.getInstance(X_509), in, offset, length);
    }

    // This is structurally more specific than what we could achieve with TrustList. There's potential in the future to
    // extend this with Function<KeyStore, CertPathTrustManagerParameters> if callers need more configurability.

    public abstract List<Certificate> certificates();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder {

        Builder addCertificates(Certificate element);

        Builder addCertificates(Certificate... elements);

        Builder addAllCertificates(Iterable<? extends Certificate> elements);

        TrustCustom build();
    }
}
