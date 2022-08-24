/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import io.deephaven.jpy.JpyConfig.Flag;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import org.jpy.PyLib;
import org.jpy.PyLibInitializer;

public class JpyConfigExt {

    private final JpyConfig config;
    private boolean initialized;

    public JpyConfigExt(JpyConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.initialized = false;
    }

    private static String safeToString(Path path) {
        return path == null ? null : path.toString();
    }

    /**
     * Equivalent to {@code logOutput.append(safeToString(path))}
     */
    private static void format(StringBuilder logOutput, Path path) {
        logOutput.append(safeToString(path));
    }

    /**
     * Appends each item from {@code collection} to the {@code logOutput}, separated by a comma
     */
    private static <T> void format(StringBuilder logOutput, Collection<T> collection) {
        boolean first = true;
        for (T item : collection) {
            if (!first) {
                logOutput.append(',');
            }
            logOutput.append(item.toString());
            first = false;
        }
    }

    public void initPython() {
        synchronized (JpyConfigExt.class) {
            if (PyLibInitializer.isPyLibInitialized()) {
                throw new IllegalStateException("PyLib has already been initialized");
            }
            if (initialized) {
                throw new IllegalStateException(
                        "Already initialized - this should not happen, unless there is some weird class unloading going on?");
            }
            PyLibInitializer.initPyLib(
                    config.getPythonLib().map(Path::toString).orElse(null),
                    config.getJpyLib().map(Path::toString).orElse(null),
                    config.getJdlLib().map(Path::toString).orElse(null));
            initialized = true;
        }
    }

    public void startPython() {
        synchronized (JpyConfigExt.class) {
            if (!PyLibInitializer.isPyLibInitialized()) {
                throw new IllegalStateException("PyLib has not been initialized");
            }
            if (!initialized) {
                throw new IllegalStateException("PyLib has been initialized, but not by the current JpyConfigExt!");
            }
        }
        if (PyLib.isPythonRunning()) {
            throw new IllegalStateException("Python is already running");
        }
        config.getProgramName().map(Path::toString).ifPresent(PyLib::setProgramName);
        config.getPythonHome().map(Path::toString).ifPresent(PyLib::setPythonHome);
        int bitset = Flag.OFF.bitset;
        for (Flag flag : config.getFlags()) {
            bitset |= flag.bitset;
        }
        PyLib.startPython(bitset, config.getExtraPaths().stream().map(Path::toString).toArray(String[]::new));
    }

    public void stopPython(Duration cleanupTimeout) {
        synchronized (JpyConfigExt.class) {
            if (!PyLibInitializer.isPyLibInitialized()) {
                throw new IllegalStateException("PyLib has not been initialized");
            }
            if (!initialized) {
                throw new IllegalStateException("PyLib has been initialized, but not by the current JpyConfigExt!");
            }
        }
        if (!PyLib.isPythonRunning()) {
            throw new IllegalStateException("Python is not running");
        }
        System.gc(); // let's try and make sure we cleanup any dangling PyObjects
        PyLib.stopPython();
        if (PyLib.isPythonRunning()) {
            throw new IllegalStateException("Python did not stop!");
        }
    }

    public void append(StringBuilder logOutput) {
        logOutput.append("flags=");
        format(logOutput, config.getFlags());
        logOutput.append(",programName=");
        format(logOutput, config.getProgramName().orElse(null));
        logOutput.append(",pythonHome=");
        format(logOutput, config.getPythonHome().orElse(null));
        logOutput.append(",pythonLib=");
        format(logOutput, config.getPythonLib().orElse(null));
        logOutput.append(",jpyLib=");
        format(logOutput, config.getJpyLib().orElse(null));
        logOutput.append(",jdlLib=");
        format(logOutput, config.getJdlLib().orElse(null));
        logOutput.append(",extras=");
        format(logOutput, config.getExtraPaths());
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        append(sb);
        return sb.toString();
    }
}
