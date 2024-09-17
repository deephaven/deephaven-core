//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpHeader;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Removes the if-modified-since header from any request that contains it.
 */
public class DropIfModifiedSinceHeader extends HttpFilter {

    @Override
    protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        String ifModifiedSince = HttpHeader.IF_MODIFIED_SINCE.asString();
        if (req.getHeader(ifModifiedSince) != null) {
            chain.doFilter(new HeaderRemovingHttpServletRequestWrapper(req, ifModifiedSince), res);
        } else {
            chain.doFilter(req, res);
        }
    }

    static class HeaderRemovingHttpServletRequestWrapper extends HttpServletRequestWrapper {

        private final String headerToRemove;

        /**
         * Constructs a request object wrapping the given request.
         *
         * @param request the {@link HttpServletRequest} to be wrapped.
         * @throws IllegalArgumentException if the request is null
         */
        public HeaderRemovingHttpServletRequestWrapper(HttpServletRequest request, String headerToRemove) {
            super(request);
            this.headerToRemove = headerToRemove;
        }

        @Override
        public String getHeader(String name) {
            if (name.equalsIgnoreCase(headerToRemove)) {
                return null;
            }
            return super.getHeader(name);
        }

        @Override
        public long getDateHeader(String name) {
            if (name.equalsIgnoreCase(headerToRemove)) {
                return -1;
            }
            return super.getDateHeader(name);
        }

        @Override
        public int getIntHeader(String name) {
            if (name.equalsIgnoreCase(headerToRemove)) {
                return -1;
            }
            return super.getIntHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            if (name.equalsIgnoreCase(headerToRemove)) {
                return Collections.emptyEnumeration();
            }
            return super.getHeaders(name);
        }

        @Override
        public Enumeration<String> getHeaderNames() {
            List<String> replacement = Collections.list(super.getHeaderNames());
            replacement.remove(headerToRemove);
            return Collections.enumeration(replacement);
        }
    }
}
