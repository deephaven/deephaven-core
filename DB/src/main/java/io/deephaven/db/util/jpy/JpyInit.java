package io.deephaven.db.util.jpy;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.jpy.JpyConfigExt;
import org.jpy.PyLibInitializer;

/**
 * Initializes Jpy.
 *
 * A guarded initialization via {@link JpyConfigExt#startPython()}
 */
public class JpyInit {

    public static void init(Logger log) {
        init(log, new JpyConfigExt(new JpyConfigLoader(Configuration.getInstance()).asJpyConfig()));
    }

    public static synchronized void init(Logger log, JpyConfigExt jpyConfig) {
        if (PyLibInitializer.isPyLibInitialized()) {
            log.warn().append("Skipping initialization of Jpy, already initialized").endl();
            log.warn().append("Using Python Installation ")
                .append(System.getProperty("jpy.pythonLib", "(unknown)")).endl();
            return;
        }

        log.info().append("Loaded jpy config ").append(jpyConfig).endl();
        log.info().append("Starting Python interpreter").endl();
        jpyConfig.initPython();
        jpyConfig.startPython();
        log.info().append("Started Python interpreter").endl();
        log.info().append("Using Python Installation ")
            .append(System.getProperty("jpy.pythonLib", "(unknown)")).endl();
    }
}
