//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

public interface Callback<T, F> {
    void onSuccess(T value);

    void onFailure(F error);
}
