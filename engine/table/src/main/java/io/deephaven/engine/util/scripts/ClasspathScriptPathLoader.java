/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.scripts;

import io.deephaven.base.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

public class ClasspathScriptPathLoader implements ScriptPathLoader {

    @Override
    public void lock() {}

    @Override
    public void unlock() {}

    @Override
    public Set<String> getAvailableScriptDisplayPaths() {
        return Collections.emptySet();
    }

    @Override
    public String getScriptBodyByDisplayPath(final String displayPath) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScriptBodyByRelativePath(final String relativePath) throws IOException {
        final InputStream stream = getClass().getResourceAsStream('/' + relativePath);
        return stream == null ? null : FileUtils.readTextFile(stream);
    }

    @Override
    public void refresh() {}

    @Override
    public void close() {}
}
