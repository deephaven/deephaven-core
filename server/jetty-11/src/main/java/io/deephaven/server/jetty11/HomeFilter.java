//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class HomeFilter implements Filter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest && response instanceof HttpServletResponse) {
            String contextPath = req.getContextPath(); // e.g. "/deephaven" or "" if root
            String queryString = req.getQueryString();

            StringBuilder location = new StringBuilder();
            location.append(contextPath.isEmpty() ? "" : contextPath).append("/ide/");

            if (queryString != null) {
                location.append('?').append(queryString);
            }

            resp.sendRedirect(location.toString());
            return;
        }
        chain.doFilter(request, response);
    }
}
