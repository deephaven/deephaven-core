/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

public interface ApplicationService {

    interface Listener {

        void onNext(FieldChanges fields);

        void onError(Throwable t);

        void onCompleted();
    }

    interface Cancel {
        void cancel();
    }

    Cancel subscribeToFields(Listener listener);
}
