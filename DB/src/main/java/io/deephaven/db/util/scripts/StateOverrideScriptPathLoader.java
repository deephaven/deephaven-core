package io.deephaven.db.util.scripts;

/**
 * An extension to the ScriptPathLoader that allows the state to be overridden.
 */
public interface StateOverrideScriptPathLoader extends ScriptPathLoader {
    /**
     * Sets a state that should be used for loading operations instead of the state the loader was
     * created with.
     *
     * @param state the state to use for loading operations.
     */
    void setOverrideState(final ScriptPathLoaderState state);

    /**
     * Instead of using the previously set override, use the state the loader was created with.
     */
    void clearOverride();

    /**
     * Get the currently active state
     * 
     * @return the currently active state
     */
    ScriptPathLoaderState getUseState();
}
