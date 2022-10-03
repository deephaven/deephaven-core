package io.deephaven.extensions.barrage;

import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * Resolves the runtime version of Barrage, in case it has been updated on the classpath beyond what this project was built with. Should be invoked before it will first be requested to ensure it is set properly.
 */
public class VersionLookup {
    public static void lookup() {
        String version;

        try (InputStream is = BarrageMessageWrapper.class.getResourceAsStream("/META-INF/maven/io.deephaven.barrage/barrage-format/pom.properties")) {
            Properties properties = new Properties();
            properties.load(is);
            version = properties.getProperty("version");
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load barrage version, is it on the classpath?", e);
        }
        System.setProperty("barrage.version", version);
    }
}
