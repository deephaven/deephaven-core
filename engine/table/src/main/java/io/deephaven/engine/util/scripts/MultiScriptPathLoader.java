/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util.scripts;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A {@link ScriptPathLoader} that will load paths from multiple underlying loaders.
 */
public class MultiScriptPathLoader<LOADER_TYPE extends ScriptPathLoader> implements ScriptPathLoader {

    private final List<LOADER_TYPE> loaders;
    private volatile SoftReference<Set<String>> availableScriptDisplayPathsReference;

    private static class MultiPathLoaderState implements ScriptPathLoaderState {
        final List<ScriptPathLoaderState> states;

        MultiPathLoaderState(final List<? extends ScriptPathLoader> stateLoaders) {
            states = new ArrayList<>(stateLoaders.size());
            stateLoaders.forEach(loader -> states.add(loader.getState()));
        }

        @Override
        public String toAbbreviatedString() {
            final String stateString = states.stream()
                    .map(state -> (state == null) ? "--" : state.toAbbreviatedString())
                    .collect(Collectors.joining(" , "));
            return '[' + stateString + ']';
        }

        @Override
        public String toString() {
            return states.toString();
        }
    }

    @SuppressWarnings("WeakerAccess")
    public MultiScriptPathLoader(@NotNull final List<LOADER_TYPE> loaders) {
        this.loaders = Collections.unmodifiableList(loaders);
    }

    @SuppressWarnings("WeakerAccess")
    public List<LOADER_TYPE> getLoaders() {
        return loaders;
    }

    @Override
    public void lock() {
        for (final ScriptPathLoader loader : loaders) {
            loader.lock();
        }
    }

    @Override
    public void unlock() {
        for (final ScriptPathLoader loader : loaders) {
            loader.unlock();
        }
    }

    @Override
    public Set<String> getAvailableScriptDisplayPaths() {
        SoftReference<Set<String>> localRef;
        Set<String> localSet;

        if (((localRef = availableScriptDisplayPathsReference) == null) || (localSet = localRef.get()) == null) {
            synchronized (this) {
                if (((localRef = availableScriptDisplayPathsReference) == null)
                        || (localSet = localRef.get()) == null) {
                    localSet = new HashSet<>();
                    for (final ScriptPathLoader loader : loaders) {
                        localSet.addAll(loader.getAvailableScriptDisplayPaths());
                    }
                    availableScriptDisplayPathsReference =
                            new SoftReference<>(localSet = Collections.unmodifiableSet(localSet));
                }
            }
        }

        return localSet;
    }

    @Override
    public String getScriptBodyByDisplayPath(final String displayPath) throws IOException {
        for (final ScriptPathLoader loader : loaders) {
            final String result = loader.getScriptBodyByDisplayPath(displayPath);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public String getScriptBodyByRelativePath(final String relativePath) throws IOException {
        for (final ScriptPathLoader loader : loaders) {
            final String result = loader.getScriptBodyByRelativePath(relativePath);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    @Override
    public Set<String> getAvailableScriptDisplayPaths(final ScriptPathLoaderState state) throws IOException {
        if (!hasStateInfo(state)) {
            return getAvailableScriptDisplayPaths();
        }

        final Set<String> paths = new HashSet<>();
        for (int i = 0; i < loaders.size(); i++) {
            paths.addAll(loaders.get(i).getAvailableScriptDisplayPaths(((MultiPathLoaderState) state).states.get(i)));
        }

        return paths;
    }

    @Override
    public String getScriptBodyByDisplayPath(final String displayPath, final ScriptPathLoaderState state)
            throws IOException {
        if (!hasStateInfo(state)) {
            return getScriptBodyByDisplayPath(displayPath);
        }

        for (int i = 0; i < loaders.size(); i++) {
            final String result = loaders.get(i).getScriptBodyByDisplayPath(displayPath,
                    ((MultiPathLoaderState) state).states.get(i));
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public String getScriptBodyByRelativePath(final String relativePath, final ScriptPathLoaderState state)
            throws IOException {
        if (!hasStateInfo(state)) {
            return getScriptBodyByRelativePath(relativePath);
        }

        for (int i = 0; i < loaders.size(); i++) {
            final String result = loaders.get(i).getScriptBodyByRelativePath(relativePath,
                    ((MultiPathLoaderState) state).states.get(i));
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    private boolean hasStateInfo(final ScriptPathLoaderState state) {
        if (state == null) {
            return false;
        }

        if (!(state instanceof MultiPathLoaderState)) {
            throw new IllegalArgumentException(
                    "Incorrect state type (" + state.getClass() + ") for MultiScriptPathLoader");
        }

        return true;
    }

    @Override
    public ScriptPathLoaderState getState() {
        return new MultiPathLoaderState(loaders);
    }

    @Override
    public void refresh() {
        availableScriptDisplayPathsReference = null;
    }

    @Override
    public void close() {
        availableScriptDisplayPathsReference = null;
    }
}
