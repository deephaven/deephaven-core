//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import io.deephaven.configuration.Configuration;
import io.grpc.servlet.jakarta.ServletAdapter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import java.io.IOException;

import static io.grpc.internal.GrpcUtil.CONTENT_TYPE_GRPC;

/**
 * Deephaven-core's own handler for registering handlers for various grpc endpoints.
 */
public class GrpcFilter extends HttpFilter {
    /**
     * Disabling this configuration option allows a server to permit http/1.1 connections. While technically forbidden
     * for grpc calls, it could be helpful for extremely lightweight http clients (IoT use cases), or for grpc-web where
     * http/1.1 is technically supported.
     */
    public static final boolean REQUIRE_HTTP2 =
            Configuration.getInstance().getBooleanWithDefault("http.requireHttp2", true);

    private final ServletAdapter grpcAdapter;

    @Inject
    public GrpcFilter(ServletAdapter grpcAdapter) {
        this.grpcAdapter = grpcAdapter;
    }

    @Override
    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (ServletAdapter.isGrpc(request)) {
            // we now know that this request is meant to be grpc, ensure that the underlying http version is not
            // 1.1 so that grpc will behave
            if (!REQUIRE_HTTP2 || request.getProtocol().equals("HTTP/2.0")) {
                grpcAdapter.doPost(request, response);
            } else {
                // A "clean" implementation of this would use @Internal-annotated types from grpc, which is discouraged.
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType(CONTENT_TYPE_GRPC);
                // Status.Code.INTERNAL.valueAscii() is private, using a string literal instead for "13"
                response.setHeader("grpc-status", "13");
                response.setHeader("grpc-message",
                        "The server connection is not using http2, so streams and metadata may not behave as expected. The client may be connecting improperly, or a proxy may be interfering.");
            }
        } else {
            chain.doFilter(request, response);
        }
    }

    public <T> T create(ServletAdapter.AdapterConstructor<T> constructor) {
        return grpcAdapter.otherAdapter(constructor);
    }
}
