package io.deephaven.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;

/**
 * Base class for server implementations to offer their own hook to read the current certificates
 */
public abstract class AbstractMtlsClientCertificateInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        return getTransportCertificates(call)
                .map(x509Certificates -> Contexts.interceptCall(
                        Context.current().withValue(MTlsCertificate.CLIENT_CERTIFICATES, x509Certificates),
                        call,
                        headers,
                        next))
                .orElseGet(() -> next.startCall(call, headers));
    }

    protected abstract <ReqT, RespT> Optional<List<X509Certificate>> getTransportCertificates(
            ServerCall<ReqT, RespT> call);
}
