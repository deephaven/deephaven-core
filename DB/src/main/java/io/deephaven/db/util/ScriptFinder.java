package io.deephaven.db.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

class ScriptFinder {
    final private String defaultScriptPath;

    static class FileOrStream {
        FileOrStream(File file) {
            this.file = Require.neqNull(file, "file");
            this.stream = null;
            this.isFile = true;
        }

        FileOrStream(InputStream stream) {
            this.file = null;
            this.stream = Require.neqNull(stream, "stream");
            this.isFile = false;
        }


        Optional<File> getFile() {
            if (isFile) {
                return Optional.of(Require.neqNull(file, "file"));
            } else {
                return Optional.empty();
            }
        }

        Optional<InputStream> getStream() {
            if (isFile) {
                return Optional.empty();
            } else {
                return Optional.of(Require.neqNull(stream, "stream"));
            }
        }

        private final boolean isFile;
        private final File file;
        private final InputStream stream;
    }

    ScriptFinder(String defaultScriptPath) {
        this.defaultScriptPath = defaultScriptPath;
    }


    InputStream findScript(final String script) throws IOException {
        return findScript(script, null);
    }

    FileOrStream findScriptEx(final String script) throws IOException {
        return findScriptEx(script, null);
    }

    InputStream findScript(final String script, final String dbScriptPath) throws IOException {
        final FileOrStream fileOrStream = findScriptEx(script, dbScriptPath);
        final Optional<InputStream> streamOptional = fileOrStream.getStream();
        if (streamOptional.isPresent()) {
            return streamOptional.get();
        } else {
            final Optional<File> fileOptional = fileOrStream.getFile();
            Assert.assertion(fileOptional.isPresent(), "fileOptional.isPresent()");
            // noinspection ConstantConditions,OptionalGetWithoutIsPresent -- if we don't have a stream we must have a
            // file
            return new FileInputStream(fileOptional.get());
        }
    }

    private FileOrStream findScriptEx(final String script, final String dbScriptPath) throws IOException {
        /*
         * NB: This code is overdue for some cleanup. In practice, there are two modes: (1) local - a user runs a local
         * groovy session from IntelliJ or otherwise, and needs to find scripts under their devroot. (2) deployed - a
         * groovy session is created from deployed code, in which case scripts are only found via the classpath. I had
         * hopes for being able to do everything via the classpath, but that doesn't allow for runtime changes without
         * additional work.
         */
        final String[] paths = (dbScriptPath == null ? defaultScriptPath : dbScriptPath).split(";");

        for (String path : paths) {
            final File file = new File(path + "/" + script);

            if (file.exists() && file.isFile()) {
                return new FileOrStream(file);
            }
        }
        InputStream result = ScriptFinder.class.getResourceAsStream(script);
        if (result == null) {
            result = ScriptFinder.class.getResourceAsStream("/" + script);
        }
        if (result != null) {
            return new FileOrStream(result);
        }
        throw new IOException("Can not find script: script=" + script
                + ", dbScriptPath=" + (Arrays.toString(paths))
                + ", classpath=" + System.getProperty("java.class.path"));
    }
}
