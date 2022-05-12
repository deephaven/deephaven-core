package io.grpc.servlet.jakarta.web;

import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.http.HttpServletResponseWrapper;

import java.io.IOException;

public class GrpcWebAsyncListener implements AsyncListener {
    private final GrpcWebFilter.GrpcWebHttpResponse wrappedResponse;

    public GrpcWebAsyncListener(GrpcWebFilter.GrpcWebHttpResponse wrappedResponse) {
        this.wrappedResponse = wrappedResponse;
    }

    @Override
    public void onComplete(AsyncEvent event) throws IOException {
        // stream is ending, write trailers as headers
        wrappedResponse.finish();
    }

    @Override
    public void onTimeout(AsyncEvent event) throws IOException {

    }

    @Override
    public void onError(AsyncEvent event) throws IOException {

    }

    @Override
    public void onStartAsync(AsyncEvent event) throws IOException {

    }
}
