/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util.scripts;

import io.deephaven.base.FileUtils;
import io.deephaven.db.util.GroovyDeephavenSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;

/**
 * This loader loads *only* using {@link GroovyDeephavenSession#findScript(String)}. Consistency locking and refresh
 * methods are no-ops.
 */
public class ConsoleScriptPathLoader implements ScriptPathLoader {

    @Override
    public void lock() {}

    @Override
    public void unlock() {}

    @Override
    public Set<String> getAvailableScriptDisplayPaths() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScriptBodyByDisplayPath(@NotNull final String displayPath) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScriptBodyByRelativePath(@NotNull final String relativePath) throws IOException {
        return FileUtils.readTextFile(GroovyDeephavenSession.findScript(relativePath));
    }

    @Override
    public void refresh() {}

    @Override
    public void close() {}
}
