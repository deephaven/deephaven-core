package io.grpc.servlet.jakarta.web;

import io.grpc.internal.GrpcUtil;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Servlet filter that translates grpc-web on the fly to match what is expected by GrpcServlet.
 * This work is done in-process with no addition copies to the request or response data - only
 * the content type header and the trailer content is specially treated at this time.
 *
 * Note that grpc-web-text is not yet supported.
 */
public class GrpcWebFilter extends HttpFilter {
    public static final String CONTENT_TYPE_GRPC_WEB = GrpcUtil.CONTENT_TYPE_GRPC + "-web";

    @Override
    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (isGrpcWeb(request)) {
            // wrap the request and response to paper over the grpc-web details
            GrpcWebHttpResponse wrappedResponse = new GrpcWebHttpResponse(response);
            HttpServletRequestWrapper wrappedRequest = new HttpServletRequestWrapper(request) {
                @Override
                public String getContentType() {
                    // Adapt the content-type to replace grpc-web with grpc
                    return super.getContentType().replaceFirst(Pattern.quote(CONTENT_TYPE_GRPC_WEB),
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
                    return new DelegatingAsyncContext(delegate) {
                        @Override
                        public void complete() {
                            // Write any trailers out to the output stream as a payload, since grpc-web doesn't
                            // use proper trailers.
                            try {
                                if (wrappedResponse.trailers != null) {
                                    Map<String, String> map = wrappedResponse.trailers.get();
                                    if (map != null) {
                                        // write a payload, even for an empty set of trailers, but not for
                                        // the absence of trailers.
                                        int trailerLength = map.entrySet().stream().mapToInt(e -> e.getKey().length() + e.getValue().length() + 4).sum();
                                        ByteBuffer payload = ByteBuffer.allocate(5 + trailerLength);
                                        payload.put((byte) 0x80);
                                        payload.putInt(trailerLength);
                                        for (Map.Entry<String, String> entry : map.entrySet()) {
                                            payload.put(entry.getKey().getBytes(StandardCharsets.US_ASCII));
                                            payload.put(": ".getBytes(StandardCharsets.US_ASCII));
                                            payload.put(entry.getValue().getBytes(StandardCharsets.US_ASCII));
                                            payload.put("\r\n".getBytes(StandardCharsets.US_ASCII));
                                        }
                                        wrappedResponse.getOutputStream().write(payload.array());
                                    }
                                }
                            } catch (IOException e) {
                                // TODO reconsider this, find a better way to report
                                throw new UncheckedIOException(e);
                            }

                            // Let the superclass complete the stream so we formally close it
                            super.complete();
                        }
                    };
                }
            };

            chain.doFilter(wrappedRequest, wrappedResponse);
        } else {
            chain.doFilter(request, response);
        }
    }

    private static boolean isGrpcWeb(ServletRequest request) {
        return request.getContentType() != null && request.getContentType().startsWith(CONTENT_TYPE_GRPC_WEB);
    }

    // Technically we should throw away content-length too, but the impl won't care
    public static class GrpcWebHttpResponse extends HttpServletResponseWrapper {
        private Supplier<Map<String, String>> trailers;

        public GrpcWebHttpResponse(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setContentType(String type) {
            // Adapt the content-type to be grpc-web
            super.setContentType(
                    type.replaceFirst(Pattern.quote(GrpcUtil.CONTENT_TYPE_GRPC), CONTENT_TYPE_GRPC_WEB));
        }

        // intercept trailers and write them out as a message just before we complete
        @Override
        public void setTrailerFields(Supplier<Map<String, String>> supplier) {
            trailers = supplier;
        }

        @Override
        public Supplier<Map<String, String>> getTrailerFields() {
            return trailers;
        }
    }
}
