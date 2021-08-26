/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util.scripts;

import java.io.IOException;
import java.util.Set;

/**
 * An interface that defines a method of loading scripts from some source.
 */
public interface ScriptPathLoader {

    /**
     * Acquire a read lock. Use before invoking any of the get* methods, and hold for as long as consistency is required
     * for this loader.
     */
    void lock();

    /**
     * Release a previously acquired read lock.
     */
    void unlock();

    /**
     * @return The display paths currently available from this loader.
     *
     * @implNote Should be called under a read lock.
     */
    Set<String> getAvailableScriptDisplayPaths();

    /**
     * @param displayPath The display path to load a script for.
     *
     * @return The body of the requested script.
     *
     * @throws IOException If a problem occurred reading the script body.
     *
     * @implNote Should be called under a read lock.
     */
    String getScriptBodyByDisplayPath(final String displayPath) throws IOException;

    /**
     * @param relativePath The relative path to load a script for.
     *
     * @return The body of the requested script.
     *
     * @throws IOException If a problem occured reading the script body.
     *
     * @implNote Should be called under a read lock.
     */
    String getScriptBodyByRelativePath(final String relativePath) throws IOException;

    /**
     * Gets the display paths available from this loader when it was in the specified {@link ScriptPathLoaderState
     * state}.
     *
     * @param state The state of the loader to use when retrieving the list.
     *
     * @return A list of all display paths available when the loader was in the specified state.
     *
     * @throws IOException If a problem occurred loading the script.
     */
    default Set<String> getAvailableScriptDisplayPaths(final ScriptPathLoaderState state) throws IOException {
        return getAvailableScriptDisplayPaths();
    }

    /**
     * Get the specified script at the specified state.
     *
     * @param displayPath The display path to the script.
     * @param state The state of the loader.
     *
     * @return The script at displayPath at the specified state.
     *
     * @throws IOException If a problem occurred loading the script.
     */
    default String getScriptBodyByDisplayPath(final String displayPath, final ScriptPathLoaderState state)
            throws IOException {
        return getScriptBodyByDisplayPath(displayPath);
    }

    /**
     * Get the specified script at the specified state.
     *
     * @param relativePath The relative path to the script.
     * @param state The state of the loader.
     *
     * @return The script at relativePath at the specified state.
     *
     * @throws IOException If a problem occurred loading the script.
     */
    default String getScriptBodyByRelativePath(final String relativePath, final ScriptPathLoaderState state)
            throws IOException {
        return getScriptBodyByRelativePath(relativePath);
    }

    /**
     * Get the current state of this {@link ScriptPathLoader}.
     *
     * @return A {@link ScriptPathLoaderState} or null if this loader is stateless.
     */
    default ScriptPathLoaderState getState() {
        return null;
    }

    /**
     * Refresh the loader internally. Will respect existing read locks.
     */
    void refresh();

    /**
     * Free resources associated with this loader.
     */
    void close();
}
