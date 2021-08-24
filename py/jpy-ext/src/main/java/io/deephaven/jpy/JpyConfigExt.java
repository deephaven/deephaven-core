package io.deephaven.jpy;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.jpy.JpyConfig.Flag;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import org.jpy.PyLib;
import org.jpy.PyLibInitializer;

public class JpyConfigExt implements LogOutputAppendable {

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
    private static void format(LogOutput logOutput, Path path) {
        logOutput.append(safeToString(path));
    }

    /**
     * Appends each item from {@code collection} to the {@code logOutput}, separated by a comma
     */
    private static <T> void format(LogOutput logOutput, Collection<T> collection) {
        boolean first = true;
        for (T item : collection) {
            if (!first) {
                logOutput = logOutput.append(',');
            }
            logOutput = logOutput.append(item.toString());
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
                throw new IllegalStateException(
                    "PyLib has been initialized, but not by the current JpyConfigExt!");
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
        PyLib.startPython(bitset,
            config.getExtraPaths().stream().map(Path::toString).toArray(String[]::new));
    }

    public void stopPython(Duration cleanupTimeout) {
        synchronized (JpyConfigExt.class) {
            if (!PyLibInitializer.isPyLibInitialized()) {
                throw new IllegalStateException("PyLib has not been initialized");
            }
            if (!initialized) {
                throw new IllegalStateException(
                    "PyLib has been initialized, but not by the current JpyConfigExt!");
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

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput
            .append("flags=").append(JpyConfigExt::format, config.getFlags())
            .append(",programName=")
            .append(JpyConfigExt::format, config.getProgramName().orElse(null))
            .append(",pythonHome=")
            .append(JpyConfigExt::format, config.getPythonHome().orElse(null))
            .append(",pythonLib=").append(JpyConfigExt::format, config.getPythonLib().orElse(null))
            .append(",jpyLib=").append(JpyConfigExt::format, config.getJpyLib().orElse(null))
            .append(",jdlLib=").append(JpyConfigExt::format, config.getJdlLib().orElse(null))
            .append(",extras=").append(JpyConfigExt::format, config.getExtraPaths());
    }
}
