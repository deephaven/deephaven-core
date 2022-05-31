package io.deephaven.server.jetty;

import io.grpc.servlet.jakarta.ServletAdapter;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.inject.Inject;
import java.io.IOException;

public class GrpcFilter implements Filter {
    private final ServletAdapter grpcAdapter;

    @Inject
    public GrpcFilter(ServletAdapter grpcAdapter) {
        this.grpcAdapter = grpcAdapter;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest && ServletAdapter.isGrpc((HttpServletRequest) request)) {
            grpcAdapter.doPost((HttpServletRequest) request, (HttpServletResponse) response);
        } else {
            chain.doFilter(request, response);
        }
    }

    public <T> T create(ServletAdapter.AdapterConstructor<T> constructor) {
        return grpcAdapter.otherAdapter(constructor);
    }
}
