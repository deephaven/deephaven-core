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
