package io.grpc.servlet.jakarta.web;

import io.grpc.internal.GrpcUtil;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Servlet filter that translates grpc-web on the fly to match what is expected by GrpcServlet. This work is done
 * in-process with no addition copies to the request or response data - only the content type header and the trailer
 * content is specially treated at this time.
 * <p>
 * Note that grpc-web-text is not yet supported.
 */
public class GrpcWebFilter extends HttpFilter {
    public static final String CONTENT_TYPE_GRPC_WEB = GrpcUtil.CONTENT_TYPE_GRPC + "-web";

    @Override
    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (isGrpcWeb(request)) {
            // wrap the request and response to paper over the grpc-web details
            GrpcWebServletResponse wrappedResponse = new GrpcWebServletResponse(response);
            GrpcWebServletRequest wrappedRequest = new GrpcWebServletRequest(request, wrappedResponse);

            chain.doFilter(wrappedRequest, wrappedResponse);
        } else {
            chain.doFilter(request, response);
        }
    }

    private static boolean isGrpcWeb(ServletRequest request) {
        return request.getContentType() != null && request.getContentType().startsWith(CONTENT_TYPE_GRPC_WEB);
    }
}
