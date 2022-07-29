package io.deephaven.grpc;

import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ServerCall;
import io.grpc.ServerInterceptor;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Provides access to the Context key to read the current client cert, if any.
 */
public class MTlsCertificate {

    public static final Context.Key<List<X509Certificate>> CLIENT_CERTIFICATES = Context.key("mtls-client-certificates");

    /**
     * Default implementation for use with Grpc's own TRANSPORT_ATTR_SSL_SESSION, to easily convert to
     * non-deprecated x509 certs.
     */
    public static ServerInterceptor DEFAULT_INTERCEPTOR = new AbstractMtlsClientCertificateInterceptor() {
        @Override
        protected <ReqT, RespT> Optional<List<X509Certificate>> getTransportCertificates(ServerCall<ReqT, RespT> call) {
            SSLSession sslSession = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            if (sslSession != null) {
                try {
                    Certificate[] javaxCerts = sslSession.getPeerCertificates();
                    if (javaxCerts == null || javaxCerts.length == 0) {
                        return Optional.empty();
                    }

                    int length = javaxCerts.length;
                    List<X509Certificate> javaCerts = new ArrayList<>(length);

                    CertificateFactory cf = CertificateFactory.getInstance("X.509");

                    for (Certificate javaxCert : javaxCerts) {
                        byte[] bytes = javaxCert.getEncoded();
                        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
                        javaCerts.add((X509Certificate) cf.generateCertificate(stream));
                    }

                    return Optional.of(Collections.unmodifiableList(javaCerts));
                } catch (SSLPeerUnverifiedException pue) {
                    return Optional.empty();
                } catch (Exception e) {
//                    LOG.warn("Unable to get X509CertChain", e);
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }
    };
}
