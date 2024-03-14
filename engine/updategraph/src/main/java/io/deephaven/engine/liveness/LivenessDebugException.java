//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

/**
 * Exception class used for getting stack traces while debugging liveness instrumentation. Should never be thrown.
 */
class LivenessDebugException extends RuntimeException {

    LivenessDebugException() {}
}
