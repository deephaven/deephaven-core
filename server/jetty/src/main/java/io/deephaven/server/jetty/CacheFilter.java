package io.deephaven.server.jetty;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class CacheFilter implements Filter {
    public static final int YEAR_IN_SECONDS = 365 * 24 * 60 * 60;

    @Override
    public void doFilter(final ServletRequest request,
            final ServletResponse response,
            final FilterChain filterChain)
            throws IOException, ServletException {
        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        // set expiry to one year in the future
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR, 1);
        httpResponse.setDateHeader("Expires", calendar.getTime().getTime());
        // Note: immutable tells firefox to never revalidate as data will never change
        httpResponse.setHeader("Cache-control", "max-age=" + YEAR_IN_SECONDS + ", public, immutable");
        httpResponse.setHeader("Pragma", "");

        filterChain.doFilter(request, response);
    }

}
