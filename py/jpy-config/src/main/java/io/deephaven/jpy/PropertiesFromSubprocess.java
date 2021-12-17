package io.deephaven.jpy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class PropertiesFromSubprocess {

    static Properties properties(String pythonName, Duration timeout)
            throws IOException, InterruptedException, TimeoutException {
        final Process process;
        try (final InputStream in = PropertiesFromSubprocess.class.getResourceAsStream("introspect.py")) {
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
