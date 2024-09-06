//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import io.deephaven.grpc.AbstractMtlsClientCertificateInterceptor;
import io.grpc.ServerCall;
import io.grpc.servlet.jakarta.GrpcServlet;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

/**
 * Jetty pre-packages the certificates for us, no need to convert them
 */
public class JettyCertInterceptor extends AbstractMtlsClientCertificateInterceptor {
    @Override
    protected <ReqT, RespT> Optional<List<X509Certificate>> getTransportCertificates(ServerCall<ReqT, RespT> call) {
        return Optional.ofNullable(call.getAttributes().get(GrpcServlet.MTLS_CERTIFICATE_KEY));
    }
}
