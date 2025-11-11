//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

/**
 * Marker interface for {@link AbstractScriptSession.ScriptSessionQueryScope} that maintain thread-local state.
 */
public interface ThreadLocalScope {
    /**
     * Capture the current thread-local scope state.
     */
    Object captureScope();

    /**
     * Push a previously captured thread-local scope state to the current thread.
     *
     * @param scope the scope state to push to the current thread
     */
    void pushScope(Object scope);

    /**
     * Pop the current thread-local scope state.
     */
    void popScope();
}
