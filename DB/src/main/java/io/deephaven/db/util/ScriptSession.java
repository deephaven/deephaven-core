/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;

import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.util.liveness.LivenessNode;
import io.deephaven.db.util.liveness.ReleasableLivenessManager;
import io.deephaven.db.util.scripts.ScriptPathLoader;
import io.deephaven.db.util.scripts.ScriptPathLoaderState;
import io.deephaven.lang.parse.api.CompletionParseService;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Interface for interactive console script sessions.
 */
public interface ScriptSession extends ReleasableLivenessManager, LivenessNode {
    /**
     * Retrieve a variable from the script session's bindings.
     *
     * @param name the variable to retrieve
     * @return the variable
     * @throws QueryScope.MissingVariableException if the variable does not exist
     */
    Object getVariable(String name) throws QueryScope.MissingVariableException;

    /**
     * Retrieve a variable from the script session's bindings. If the variable is not present,
     * return defaultValue.
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
     * A {@link VariableProvider} instance, for services like autocomplete which may want a limited
     * "just the variables" view of our session state.
     * 
     * @return a VariableProvider instance backed by the global/binding context of this script
     *         session.
     */
    VariableProvider getVariableProvider();

    CompletionParseService getParser();

    class Changes {
        public Map<String, ExportedObjectType> created = new HashMap<>();
        public Map<String, ExportedObjectType> updated = new HashMap<>();
        public Map<String, ExportedObjectType> removed = new HashMap<>();
    }

    /**
     * Evaluates the script and manages liveness of objects that are exported to the user. This
     * method should be called from the serial executor as it manipulates static state.
     *
     * @param script the code to execute
     * @return the changes made to the exportable objects
     */
    default Changes evaluateScript(String script) {
        return evaluateScript(script, null);
    }

    /**
     * Evaluates the script and manages liveness of objects that are exported to the user. This
     * method should be called from the serial executor as it manipulates static state.
     *
     * @param script the code to execute
     * @param scriptName an optional script name, which may be ignored by the implementation, or
     *        used improve error messages or for other internal purposes
     * @return the changes made to the exportable objects
     */
    Changes evaluateScript(String script, @Nullable String scriptName);

    /**
     * Retrieves all of the variables present in the session's scope (e.g., Groovy binding, Python
     * globals()).
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
    void setVariable(String name, Object value);

    /**
     * @return a textual description of this script session's language for use in messages.
     */
    String scriptType();

    /**
     * If this script session can throw unserializable exceptions, this method is responsible for
     * turning those exceptions into something suitable for sending back to a client.
     *
     * @param e the exception to (possibly) sanitize
     * @return the sanitized exception
     */
    default Throwable sanitizeThrowable(Throwable e) {
        return e;
    }

    /**
     * Called before Application initialization, should setup sourcing from the controller (as
     * required).
     */
    void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoader,
        ScriptPathLoaderState scriptLoaderState);

    /**
     * Called after Application initialization.
     */
    void onApplicationInitializationEnd();

    /**
     * Sets the scriptPathLoader that is in use for this session.
     * 
     * @param scriptPathLoader a supplier of a script path loader
     * @param caching whether the source operation should cache results
     */
    void setScriptPathLoader(Supplier<ScriptPathLoader> scriptPathLoader, boolean caching);

    /**
     * Removes the currently configured script path loader from this script.
     */
    void clearScriptPathLoader();

    /**
     * Informs the session whether or not we should be using the original ScriptLoaderState for
     * source commands.
     *
     * @param useOriginal whether to use the script loader state at persistent query initialization
     */
    boolean setUseOriginalScriptLoaderState(boolean useOriginal);

    /**
     * Asks the session to remove any wrapping that exists on scoped objects so that clients can
     * fetch them. Defaults to returning the object itself.
     *
     * @param object the scoped object
     * @return an obj which can be consumed by a client
     */
    default Object unwrapObject(Object object) {
        return object;
    }
}
