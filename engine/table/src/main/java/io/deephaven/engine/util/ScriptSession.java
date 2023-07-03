/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.ReleasableLivenessManager;
import io.deephaven.engine.util.scripts.ScriptPathLoader;
import io.deephaven.engine.util.scripts.ScriptPathLoaderState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Interface for interactive console script sessions.
 */
public interface ScriptSession extends ReleasableLivenessManager, LivenessNode {

    /**
     * Retrieve a variable from the script session's bindings.
     * <p/>
     * Please use {@link ScriptSession#getVariable(String, Object)} if you expect the variable may not exist.
     *
     * @param name the variable to retrieve
     * @return the variable
     * @throws QueryScope.MissingVariableException if the variable does not exist
     */
    @NotNull
    Object getVariable(String name) throws QueryScope.MissingVariableException;

    /**
     * Retrieve a variable from the script session's bindings. If the variable is not present, return defaultValue.
     *
     * If the variable is present, but is not of type (T), a ClassCastException may result.
     *
     * @param name the variable to retrieve
     * @param defaultValue the value to use when no value is present in the session's scope
     * @param <T> the type of the variable
     * @return the value of the variable, or defaultValue if not present
     */
    <T> T getVariable(String name, T defaultValue);

    /**
     * A {@link VariableProvider} instance, for services like autocomplete which may want a limited "just the variables"
     * view of our session state.
     *
     * @return a VariableProvider instance backed by the global/binding context of this script session.
     */
    VariableProvider getVariableProvider();

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
     * Retrieves all of the variables present in the session's scope (e.g., Groovy binding, Python globals()).
     *
     * @return an unmodifiable map with variable names as the keys, and the Objects as the result
     */
    Map<String, Object> getVariables();

    /**
     * Retrieves all of the variable names present in the session's scope
     *
     * @return an unmodifiable set of variable names
     */
    Set<String> getVariableNames();

    /**
     * Check if the scope has the given variable name
     *
     * @param name the variable name
     * @return True iff the scope has the given variable name
     */
    boolean hasVariableName(String name);

    /**
     * Inserts a value into the script's scope.
     *
     * @param name the variable name to set
     * @param value the new value of the variable
     */
    void setVariable(String name, @Nullable Object value);

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
     * Called before Application initialization, should setup sourcing from the controller (as required).
     */
    @Deprecated(forRemoval = true)
    default void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader,
            ScriptPathLoaderState scriptLoaderState) {
        throw new UnsupportedOperationException("setUseOriginalScriptLoaderState deprecated for removal");
    }

    /**
     * Called after Application initialization.
     */
    @Deprecated(forRemoval = true)
    default void onApplicationInitializationEnd() {
        throw new UnsupportedOperationException("setUseOriginalScriptLoaderState deprecated for removal");
    }

    /**
     * Sets the scriptPathLoader that is in use for this session.
     *
     * @param scriptPathLoader a supplier of a script path loader
     * @param caching whether the source operation should cache results
     */
    @Deprecated(forRemoval = true)
    default void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching) {
        throw new UnsupportedOperationException("setUseOriginalScriptLoaderState deprecated for removal");
    }

    /**
     * Removes the currently configured script path loader from this script.
     */
    @Deprecated(forRemoval = true)
    default void clearScriptPathLoader() {
        throw new UnsupportedOperationException("setUseOriginalScriptLoaderState deprecated for removal");
    }

    /**
     * Informs the session whether or not we should be using the original ScriptLoaderState for source commands.
     *
     * @param useOriginal whether to use the script loader state at persistent query initialization
     */
    @Deprecated(forRemoval = true)
    default boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        throw new UnsupportedOperationException("setUseOriginalScriptLoaderState deprecated for removal");
    }

    /**
     * Asks the session to remove any wrapping that exists on scoped objects so that clients can fetch them. Defaults to
     * returning the object itself.
     *
     * @param object the scoped object
     * @return an obj which can be consumed by a client
     */
    default Object unwrapObject(Object object) {
        return object;
    }
}
