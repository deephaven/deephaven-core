package io.grpc.servlet.jakarta.web;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;

/**
 * Util class to allow the complete() call to get some work done (writing trailers as a payload) before calling the
 * actual container implementation. The container will finish closing the stream before invoking the async listener and
 * formally informing the filter that the stream has closed, making this our last chance to intercept the closing of the
 * stream before it happens.
 */
public class DelegatingAsyncContext implements AsyncContext {
    private final AsyncContext delegate;

    public DelegatingAsyncContext(AsyncContext delegate) {
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
