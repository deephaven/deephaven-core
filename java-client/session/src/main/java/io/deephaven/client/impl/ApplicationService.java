package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;

public interface ApplicationService {

    interface Listener {
        void onNext(FieldsChangeUpdate fields);

        void onError(Throwable t);

        void onCompleted();
    }

    interface Cancel {
        void cancel();
    }

    Cancel subscribeToFields(Listener listener);
}
