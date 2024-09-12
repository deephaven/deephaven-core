//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.engine.util.input.InputTableStatusListener;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jpy.PyObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@ScriptApi
public class PythonInputTableStatusListenerAdapter implements InputTableStatusListener {

    private static final Logger log = LoggerFactory.getLogger(PythonInputTableStatusListenerAdapter.class);
    private final PyObject pyOnSuccessCallable;
    private final PyObject pyOnErrorCallback;

    /**
     * Create a Python InputTable status listener.
     *
     * @param pyOnSuccessListener The Python onSuccess callback function.
     * @param pyOnErrorCallback The Python onError callback function.
     */
    private PythonInputTableStatusListenerAdapter(@Nullable PyObject pyOnSuccessListener,
            @Nonnull PyObject pyOnErrorCallback) {
        this.pyOnSuccessCallable = pyOnSuccessListener;
        this.pyOnErrorCallback = pyOnErrorCallback;
    }

    public static PythonInputTableStatusListenerAdapter create(@Nullable PyObject pyOnSuccessListener,
            @Nonnull PyObject pyOnErrorCallback) {
        return new PythonInputTableStatusListenerAdapter(pyOnSuccessListener,
                Objects.requireNonNull(pyOnErrorCallback, "Python on_error callback cannot be None"));
    }

    @Override
    public void onError(Throwable originalException) {
        if (!pyOnErrorCallback.isNone()) {
            try {
                pyOnErrorCallback.call("__call__", ExceptionUtils.getStackTrace(originalException));
            } catch (Throwable e) {
                // If the Python onFailure callback fails, log the new exception
                // and continue with the original exception.
                log.error().append("Python on_error callback failed: ").append(e).endl();
            }
        } else {
            log.error().append("Python on_error callback is None: ")
                    .append(ExceptionUtils.getStackTrace(originalException)).endl();
        }
    }

    @Override
    public void onSuccess() {
        if (pyOnSuccessCallable != null && !pyOnSuccessCallable.isNone()) {
            pyOnSuccessCallable.call("__call__");
        } else {
            InputTableStatusListener.super.onSuccess();
        }
    }
}
