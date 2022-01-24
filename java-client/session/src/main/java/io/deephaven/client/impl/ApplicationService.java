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
