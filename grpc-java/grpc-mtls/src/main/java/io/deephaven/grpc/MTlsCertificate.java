/*
 * Copyright 2022 Deephaven Data Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.deephaven.grpc;

import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.ServerCall;
import io.grpc.ServerInterceptor;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides access to the Context key to read the current client cert, if any.
 */
public class MTlsCertificate {
    private static final Logger logger = Logger.getLogger(MTlsCertificate.class.getName());

    public static final Context.Key<List<X509Certificate>> CLIENT_CERTIFICATES =
            Context.key("mtls-client-certificates");

    /**
     * Default implementation for use with Grpc's own TRANSPORT_ATTR_SSL_SESSION, to easily convert to non-deprecated
     * x509 certs.
     */
    public final static ServerInterceptor DEFAULT_INTERCEPTOR = new AbstractMtlsClientCertificateInterceptor() {
        @Override
        protected <ReqT, RespT> Optional<List<X509Certificate>> getTransportCertificates(ServerCall<ReqT, RespT> call) {
            SSLSession sslSession = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            if (sslSession == null) {
                return Optional.empty();
            }
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
            } catch (CertificateException e) {
                logger.log(Level.WARNING, "Unable to read X509CertChain due to certificate exception", e);
                return Optional.empty();
            }
        }
    };
}
