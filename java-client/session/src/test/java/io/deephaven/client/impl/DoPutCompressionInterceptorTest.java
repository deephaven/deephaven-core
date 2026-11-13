//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.uri.DeephavenTarget;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DoPutCompressionInterceptorTest {

    @Test
    void compressesOnlyDoPut() {
        final List<CallOptions> sent = new ArrayList<>();
        final Channel channel = new Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    final MethodDescriptor<ReqT, RespT> method,
                    final CallOptions callOptions) {
                sent.add(callOptions);
                return null;
            }

            @Override
            public String authority() {
                return "localhost";
            }
        };
        final DoPutCompressionInterceptor interceptor = new DoPutCompressionInterceptor("zstd");
        for (final String method : List.of("DoPut", "DoGet", "DoExchange")) {
            interceptor.interceptCall(method("arrow.flight.protocol.FlightService", method), CallOptions.DEFAULT,
                    channel);
        }
        // a DoPut of another service is not an Arrow Flight upload
        interceptor.interceptCall(method("example.Uploads", "DoPut"), CallOptions.DEFAULT, channel);

        assertThat(sent).extracting(CallOptions::getCompressor).containsExactly("zstd", null, null, null);
    }

    @Test
    void requestCompressionMustBeSupported() {
        final DeephavenTarget target = DeephavenTarget.of(URI.create("dh+plain://localhost:10000"));
        assertThat(ClientConfig.builder().target(target).build().requestCompression()).isEmpty();
        assertThat(ClientConfig.builder().target(target).requestCompression("gzip").build().requestCompression())
                .contains("gzip");
        assertThatThrownBy(() -> ClientConfig.builder().target(target).requestCompression("lz4").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lz4");
    }

    private static MethodDescriptor<Void, Void> method(final String service, final String method) {
        final MethodDescriptor.Marshaller<Void> marshaller = new MethodDescriptor.Marshaller<>() {
            @Override
            public InputStream stream(final Void value) {
                return new ByteArrayInputStream(new byte[0]);
            }

            @Override
            public Void parse(final InputStream stream) {
                return null;
            }
        };
        return MethodDescriptor.<Void, Void>newBuilder()
                .setType(MethodDescriptor.MethodType.UNKNOWN)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(service, method))
                .setRequestMarshaller(marshaller)
                .setResponseMarshaller(marshaller)
                .build();
    }
}
