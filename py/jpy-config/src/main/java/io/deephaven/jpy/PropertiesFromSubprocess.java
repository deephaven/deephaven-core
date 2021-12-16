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
                if (e.getMessage().contains("error=2, No such file or directory")) {
                    throw new IOException(String.format("Python '%s' not found.", pythonName), e);
                }
                throw e;
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
                throw new IllegalStateException("A Deephaven python environment has not been configured");
            }
            throw new IllegalStateException(error);
        }
        final Properties properties = new Properties();
        properties.load(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        return properties;
    }
}
