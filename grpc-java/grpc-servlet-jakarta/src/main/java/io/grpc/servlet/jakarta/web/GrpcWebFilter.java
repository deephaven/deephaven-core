package io.grpc.servlet.jakarta.web;

import io.grpc.internal.GrpcUtil;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletContext;
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
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

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
                    return super.startAsync(this, wrappedResponse);
                }

                @Override
                public AsyncContext startAsync(ServletRequest servletRequest, ServletResponse servletResponse)
                        throws IllegalStateException {
                    AsyncContext delegate = super.startAsync(servletRequest, servletResponse);
                    return new DelegatingAsyncContext(delegate) {
                        @Override
                        public void complete() {
                            try {
                                wrappedResponse.finish();
                            } catch (IOException e) {
                                // TODO reconsider this, find a better way to report
                                throw new UncheckedIOException(e);
                            }
                            super.complete();
                        }
                    };
                }
            };

            try {
                chain.doFilter(wrappedRequest, wrappedResponse);
            } finally {
                // if (request.isAsyncStarted()) {
                // request.getAsyncContext().addListener(new GrpcWebAsyncListener(wrappedResponse));
                // }
            }
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
        private GrpcWebServletOutputStream outputStream;

        public GrpcWebHttpResponse(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setContentType(String type) {
            // Adapt the content-type to be grpc-web
            super.setContentType(
                    type.replaceFirst(Pattern.quote(GrpcUtil.CONTENT_TYPE_GRPC), CONTENT_TYPE_GRPC_WEB));
        }

        @Override
        public GrpcWebServletOutputStream getOutputStream() throws IOException {
            if (outputStream == null) {
                outputStream = new GrpcWebServletOutputStream(super.getOutputStream());
            }
            return outputStream;
        }

        @Override
        public void flushBuffer() throws IOException {
            super.getOutputStream().flush();
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

        public void finish() throws IOException {
            // write any trailers out to the output stream
            getOutputStream().writeTrailers(trailers);
        }


    }

    private static class DelegatingAsyncContext implements AsyncContext {
        private final AsyncContext delegate;

        private DelegatingAsyncContext(AsyncContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public ServletRequest getRequest() {
            return delegate.getRequest();
        }

        @Override
        public ServletResponse getResponse() {
            return delegate.getResponse();
        }

        @Override
        public boolean hasOriginalRequestAndResponse() {
            return delegate.hasOriginalRequestAndResponse();
        }

        @Override
        public void dispatch() {
            delegate.dispatch();
        }

        @Override
        public void dispatch(String path) {
            delegate.dispatch(path);
        }

        @Override
        public void dispatch(ServletContext context, String path) {
            delegate.dispatch(context, path);
        }

        @Override
        public void complete() {
            delegate.complete();
        }

        @Override
        public void start(Runnable run) {
            delegate.start(run);
        }

        @Override
        public void addListener(AsyncListener listener) {
            delegate.addListener(listener);
        }

        @Override
        public void addListener(AsyncListener listener, ServletRequest servletRequest,
                ServletResponse servletResponse) {
            delegate.addListener(listener, servletRequest, servletResponse);
        }

        @Override
        public <T extends AsyncListener> T createListener(Class<T> clazz) throws ServletException {
            return delegate.createListener(clazz);
        }

        @Override
        public void setTimeout(long timeout) {
            delegate.setTimeout(timeout);
        }

        @Override
        public long getTimeout() {
            return delegate.getTimeout();
        }
    }
}
