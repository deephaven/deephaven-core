package io.deephaven.engine.util.liveness;

/**
 * Exception class used for getting stack traces while debugging liveness instrumentation. Should never be thrown.
 */
class LivenessDebugException extends RuntimeException {

    LivenessDebugException() {}
}
