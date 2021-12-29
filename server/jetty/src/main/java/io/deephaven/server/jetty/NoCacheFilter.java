package io.deephaven.server.jetty;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class NoCacheFilter implements Filter {
    @Override
    public void doFilter(final ServletRequest request,
            final ServletResponse response,
            final FilterChain filterChain)
            throws IOException, ServletException {
        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        // set expiry to back in the past (makes us a bad candidate for caching)
        httpResponse.setDateHeader("Expires", 0);
        // HTTP 1.0 (disable caching)
        httpResponse.setHeader("Pragma", "no-cache");
        // HTTP 1.1 (disable caching of any kind)
        // HTTP 1.1 'pre-check=0, post-check=0' => (Internet Explorer should always check)
        // Note: no-store is not included here as it will disable offline application storage on Firefox
        httpResponse.setHeader("Cache-control", "no-cache, must-revalidate, pre-check=0, post-check=0");

        filterChain.doFilter(request, response);
    }

}
