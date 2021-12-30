package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;

public interface ApplicationService {

    interface Listener {

        // TODO(deephaven-core#1783): Remove proto references from java client interface #1783
        void onNext(FieldsChangeUpdate fields);

        void onError(Throwable t);

        void onCompleted();
    }

    interface Cancel {
        void cancel();
    }

    Cancel subscribeToFields(Listener listener);
}
