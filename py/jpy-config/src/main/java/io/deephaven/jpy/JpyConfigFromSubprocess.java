package io.deephaven.jpy;

import io.deephaven.jpy.JpyConfigSource.FromProperties;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class JpyConfigFromSubprocess {

    private static final String PYTHON_NAME_PROPERTY = "deephaven.python.name";

    private static final String VIRTUAL_ENV = "VIRTUAL_ENV";

    private static final String PYTHON_NAME = "python3";

    /**
     * Find the "best" python name. If the system property {@value PYTHON_NAME_PROPERTY} is set, return it. Otherwise,
     * if the environment variable {@value VIRTUAL_ENV} is set, return "${VIRTUAL_ENV}/bin/python". Otherwise, return
     * {@value PYTHON_NAME}.
     *
     * @return the python name
     */
    public static String getPythonName() {
        final String deephavenPythonName = System.getProperty(PYTHON_NAME_PROPERTY);
        if (deephavenPythonName != null) {
            return deephavenPythonName;
        }
        final String virtualEnv = System.getenv(VIRTUAL_ENV);
        if (virtualEnv != null) {
            return virtualEnv + "/bin/python";
        }
        return PYTHON_NAME;
    }

    /**
     * Equivalent to {@code fromSubprocess(getPythonName(), timeout)}.
     *
     * @param timeout the timeout
     * @return the jpy configuration, based on a python execution in the current environment
     * @throws IOException if an IO exception occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the command to execute
     * @throws TimeoutException if the command times out
     * @see #getPythonName()
     * @see #fromSubprocess(String, Duration)
     */
    public static JpyConfigSource fromSubprocess(Duration timeout)
            throws IOException, InterruptedException, TimeoutException {
        return fromSubprocess(getPythonName(), timeout);
    }

    /**
     * Create the configuration based off of a python subprocess that introspects itself. The process will be executed
     * based off of {@code pythonName}, which may be a path, or will otherwise be sourced from the environment PATH.
     *
     * @param pythonName the python command
     * @param timeout the timeout
     * @return the jpy configuration, based on a python execution in the current environment
     * @throws IOException if an IO exception occurs
     * @throws InterruptedException if the current thread is interrupted while waiting for the command to execute
     * @throws TimeoutException if the command times out
     */
    public static JpyConfigSource fromSubprocess(String pythonName, Duration timeout)
            throws IOException, InterruptedException, TimeoutException {
        return new FromProperties(properties(pythonName, timeout));
    }

    private static Properties properties(String pythonName, Duration timeout)
            throws IOException, InterruptedException, TimeoutException {
        final Process process;
        try (final InputStream in = JpyConfigFromSubprocess.class.getResourceAsStream("introspect.py")) {
            if (in == null) {
                throw new IllegalStateException("Expected to find introspect.py resource");
            }
            // The "-" means that python will read script from stdin
            try {
                process = new ProcessBuilder(pythonName, "-").start();
            } catch (IOException e) {
                throw new IOException(String.format("Error starting python command '%s'", pythonName), e);
            }
            // We need to also close the stream to let python know it can start processing script
            try (OutputStream out = process.getOutputStream()) {
                in.transferTo(out);
            } catch (Throwable t) {
                process.destroy();
                throw t;
            }
        }
        try {
            if (!process.waitFor(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                throw new TimeoutException(String.format("Timed out while waiting for '%s' to complete", pythonName));
            }
        } catch (Throwable t) {
            process.destroy();
            throw t;
        }
        final int exitValue = process.exitValue();
        if (exitValue != 0) {
            final String error = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            if (error.contains("ModuleNotFoundError: No module named 'jpyutil'")) {
                throw new IllegalStateException(
                        String.format("A Deephaven python environment has not been configured for '%s'. " +
                                "Please ensure that the appropriate Deephaven wheels have been installed.",
                                pythonName));
            }
            throw new IllegalStateException(
                    String.format("Unexpected error while starting python '%s': %s", pythonName, error));
        }
        final Properties properties = new Properties();
        properties.load(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        return properties;
    }
}
