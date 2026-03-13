//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.server.session.SessionService;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps an underlying ErrorTransformer but records all errors that occur in a list.
 */
@Singleton
public class RecordingErrorTransformer implements SessionService.ErrorTransformer {
    private final SessionService.ErrorTransformer wrapped;
    private final List<Throwable> errorList = new ArrayList<>();

    @Inject
    public RecordingErrorTransformer(SessionService.ObfuscatingErrorTransformer wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public StatusRuntimeException transform(Throwable t) {
        synchronized (this) {
            errorList.add(t);
        }
        return wrapped.transform(t);
    }

    /**
     * Clears the recorded errors.
     */
    public synchronized void clear() {
        errorList.clear();
    }

    /**
     * @return a copy of the recorded errors
     */
    public synchronized List<Throwable> getErrors() {
        return new ArrayList<>(errorList);
    }
}
