package io.deephaven.web.shared.fu;

/**
 * A useful interface for "promise-like" objects, (actually, our LazyPromise class), so we can
 * expose utility functions that know how to deal with these objects, without actually putting them
 * on the shared classpath.
 */
public interface PromiseLike<T> {

    String CANCELLATION_MESSAGE = "User cancelled request";

    void onSuccess(JsConsumer<T> success);

    void onFailure(JsConsumer<Object> failure);
}
