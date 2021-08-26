package io.deephaven.web.client.api;

public interface Callback<T, F> {
    void onSuccess(T value);

    void onFailure(F error);
}
