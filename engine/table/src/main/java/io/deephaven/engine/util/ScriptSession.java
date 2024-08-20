//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.ReleasableLivenessManager;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Interface for interactive console script sessions.
 */
public interface ScriptSession extends LivenessNode {

    /**
     * Provides access to the query scope defined by the state in this script session.
     *
     * @return an implementation defined QueryScope, allowing access to state in the script session
     */
    QueryScope getQueryScope();

    /**
     * Obtain an {@link ExecutionContext} instance for the current script session. This is the execution context that is
     * used when executing scripts.
     */
    ExecutionContext getExecutionContext();

    class Changes {
        public RuntimeException error = null;

        // TODO(deephaven-core#1781): Close gaps between proto "CustomType" fields

        public Map<String, String> created = new LinkedHashMap<>();
        public Map<String, String> updated = new LinkedHashMap<>();
        public Map<String, String> removed = new LinkedHashMap<>();

        public boolean isEmpty() {
            return error == null && created.isEmpty() && updated.isEmpty() && removed.isEmpty();
        }

        public void throwIfError() {
            if (error != null) {
                throw error;
            }
        }
    }

    interface Listener {
        void onScopeChanges(ScriptSession scriptSession, Changes changes);
    }

    /**
     * Observe (and report via {@link Listener#onScopeChanges(ScriptSession, Changes) onScopeChanges}) any changes to
     * this ScriptSession's {@link QueryScope} that may have been made externally, rather than during
     * {@link #evaluateScript script evaluation}.
     * 
     * @apiNote This method should be regarded as an unstable API
     */
    void observeScopeChanges();

    /**
     * Evaluates the script and manages liveness of objects that are exported to the user. This method should be called
     * from the serial executor as it manipulates static state.
     *
     * @param script the code to execute
     * @return the changes made to the exportable objects
     */
    default Changes evaluateScript(String script) {
        return evaluateScript(script, null);
    }

    /**
     * Evaluates the script and manages liveness of objects that are exported to the user. This method should be called
     * from the serial executor as it manipulates static state.
     *
     * @param script the code to execute
     * @param scriptName an optional script name, which may be ignored by the implementation, or used improve error
     *        messages or for other internal purposes
     * @return the changes made to the exportable objects
     */
    Changes evaluateScript(String script, @Nullable String scriptName);

    /**
     * Evaluates the script and manages liveness of objects that are exported to the user. This method should be called
     * from the serial executor as it manipulates static state.
     *
     * @param scriptPath the path to the script to execute
     * @return the changes made to the exportable objects
     */
    Changes evaluateScript(Path scriptPath);

    /**
     * @return a textual description of this script session's language for use in messages.
     */
    String scriptType();

    /**
     * If this script session can throw unserializable exceptions, this method is responsible for turning those
     * exceptions into something suitable for sending back to a client.
     *
     * @param e the exception to (possibly) sanitize
     * @return the sanitized exception
     */
    default Throwable sanitizeThrowable(Throwable e) {
        return e;
    }

    /**
     * Asks the session to remove any wrapping that exists on scoped objects so that clients can fetch them. Defaults to
     * returning the object itself.
     *
     * @param object the scoped object
     * @return an obj which can be consumed by a client
     */
    default Object unwrapObject(@Nullable Object object) {
        return object;
    }
}
