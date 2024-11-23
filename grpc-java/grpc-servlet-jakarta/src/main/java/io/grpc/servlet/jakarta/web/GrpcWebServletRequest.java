/*
 * Copyright 2019 The gRPC Authors
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

package io.grpc.servlet.jakarta.web;

import io.grpc.internal.GrpcUtil;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Wraps an incoming gRPC-web request so that a downstream filter/servlet can read it instead as a gRPC payload. This
 * currently involves changing the incoming content-type, and managing the wrapped request so that downstream operations
 * to handle this request behave correctly.
 */
public class GrpcWebServletRequest extends HttpServletRequestWrapper {
    private static final Logger logger = Logger.getLogger(GrpcWebServletRequest.class.getName());

    private final GrpcWebServletResponse wrappedResponse;

    public GrpcWebServletRequest(HttpServletRequest request, GrpcWebServletResponse wrappedResponse) {
        super(request);
        this.wrappedResponse = wrappedResponse;
    }

    @Override
    public String getContentType() {
        // Adapt the content-type to replace grpc-web with grpc
        return super.getContentType().replaceFirst(Pattern.quote(GrpcWebFilter.CONTENT_TYPE_GRPC_WEB),
                GrpcUtil.CONTENT_TYPE_GRPC);
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        return startAsync(this, wrappedResponse);
    }

    @Override
    public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
            throws IllegalStateException {
        AsyncContext delegate = super.startAsync(servletRequest, servletResponse);
        // Note that this anonymous class has no purpose while our workaround for
        // https://github.com/deephaven/deephaven-core/issues/6400 is in place.
        return new DelegatingAsyncContext(delegate) {
            private void safelyComplete() {
                try {
                    // Let the superclass complete the stream so we formally close it
                    super.complete();
                } catch (Exception e) {
                    // As above, complete() should not throw, so just log this failure and continue.
                    // This statement is somewhat dubious, since Jetty itself is clearly throwing in
                    // complete()... leading us to add this try/catch to begin with.
                    logger.log(Level.FINE, "Error invoking complete() on underlying stream", e);
                }

            }

            @Override
            public void complete() {
                // Emit trailers as part of the response body, then complete the request. Note that this may mean
                // that we don't actually call super.complete() synchronously.
                wrappedResponse.writeTrailers(this::safelyComplete);
            }
        };
    }
}
