package io.deephaven.engine.util.jpy;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.jpy.JpyConfig;
import io.deephaven.jpy.JpyConfigExt;
import io.deephaven.jpy.JpyConfigFromSubprocess;
import io.deephaven.jpy.JpyConfigSource;
import org.jpy.PyLibInitializer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Initializes Jpy.
 *
 * A guarded initialization via {@link JpyConfigExt#startPython()}
 */
public class JpyInit {

    /**
     * First, checks {@link Configuration#getInstance()} for any explicit jpy properties. If none are set, it will
     * source the configuration from {@link JpyConfigFromSubprocess#fromSubprocess(Duration)}.
     *
     * @param log the log
     * @throws IOException if an IO exception occurs
     * @throws InterruptedException if the current thread is interrupted
     * @throws TimeoutException if the command times out
     */
    public static void init(Logger log) throws IOException, InterruptedException, TimeoutException {
        final JpyConfig explicitConfig = new JpyConfigLoader(Configuration.getInstance()).asJpyConfig();
        if (!explicitConfig.isEmpty()) {
            init(log, new JpyConfigExt(explicitConfig));
            return;
        }
        final JpyConfigSource fromSubprocess = JpyConfigFromSubprocess.fromSubprocess(Duration.ofSeconds(10));
        init(log, new JpyConfigExt(fromSubprocess.asJpyConfig()));
    }

    public static synchronized void init(Logger log, JpyConfigExt jpyConfig) {
        if (PyLibInitializer.isPyLibInitialized()) {
            log.warn().append("Skipping initialization of Jpy, already initialized").endl();
            log.warn().append("Using Python Installation ").append(System.getProperty("jpy.pythonLib", "(unknown)"))
                    .endl();
            return;
        }

        log.info().append("Loaded jpy config ").append(jpyConfig).endl();
        log.info().append("Starting Python interpreter").endl();
        jpyConfig.initPython();
        jpyConfig.startPython();
        log.info().append("Started Python interpreter").endl();
        log.info().append("Using Python Installation ").append(System.getProperty("jpy.pythonLib", "(unknown)")).endl();
    }
}
