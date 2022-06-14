/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utilities for working with {@link Properties}.
 */
public class PropertiesUtil {

    /**
     * Load the properties file from {@code file} using {@link StandardCharsets#UTF_8 UTF-8}.
     *
     * @param file the file
     * @return the properties
     * @throws IOException if an IO exception occurs
     */
    public static Properties load(String file) throws IOException {
        final Properties properties = new Properties();
        load(properties, Paths.get(file), StandardCharsets.UTF_8);
        return properties;
    }

    /**
     * Load the properties from {@code path} into {@code properties} with {@code charset}.
     *
     * @param properties the properties
     * @param path the path
     * @param charset the charset
     * @throws IOException if an IO exception occurs
     */
    public static void load(Properties properties, Path path, Charset charset) throws IOException {
        try (final BufferedReader reader = Files.newBufferedReader(path, charset)) {
            properties.load(reader);
        }
    }
}
