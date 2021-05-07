/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jpy;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.Set;

/**
 * Provides configuration for {@link org.jpy.PyLib}.
 *
 * @author Norman Fomferra
 * @since 0.7
 */
class PyLibConfig {

    private static final boolean DEBUG = Boolean.getBoolean("jpy.debug");
    public static final String PYTHON_LIB_KEY = "jpy.pythonLib";
    public static final String JPY_LIB_KEY = "jpy.jpyLib";
    public static final String JDL_LIB_KEY = "jpy.jdlLib";
    public static final String JPY_CONFIG_KEY = "jpy.config";
    public static final String JPY_CONFIG_RESOURCE = "jpyconfig.properties";

    public enum OS {
        WINDOWS,
        UNIX,
        MAC_OS,
        SUNOS,
    }

    private static final Properties properties = new Properties();


    static {
        if (DEBUG) System.out.println("org.jpy.PyLibConfig: entered static initializer");
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(JPY_CONFIG_RESOURCE);
        if (stream != null) {
            loadConfig(stream, JPY_CONFIG_RESOURCE);
        }
        String path = System.getProperty(JPY_CONFIG_KEY);
        if (path != null) {
            File file = new File(path);
            if (file.isFile()) {
                loadConfig(file);
            }
        }
        File file = new File(JPY_CONFIG_RESOURCE).getAbsoluteFile();
        if (file.isFile()) {
            loadConfig(file);
        }
        if (DEBUG) System.out.println("org.jpy.PyLibConfig: exited static initializer");
    }

    private static void loadConfig(InputStream stream, String name) {
        try {
            if (DEBUG)
                System.out.printf(String.format("org.jpy.PyLibConfig: loading configuration resource %s\n", name));
            try (Reader reader = new InputStreamReader(stream)) {
                loadConfig(reader);
            }
        } catch (IOException e) {
            if (DEBUG) e.printStackTrace(System.err);
        }
    }

    private static void loadConfig(File file) {
        try {
            if (DEBUG)
                System.out.printf(String.format("%s: loading configuration file %s\n", PyLibConfig.class.getName(), file));
            try (Reader reader = new FileReader(file)) {
                loadConfig(reader);
            }
        } catch (IOException e) {
            System.err.printf("org.jpy.PyLibConfig: %s: %s\n", file, e.getMessage());
            if (DEBUG) e.printStackTrace(System.err);
        }
    }

    private static void loadConfig(Reader reader) throws IOException {
        properties.load(reader);
        Set<String> propertyNames = properties.stringPropertyNames();
        for (String propertyName : propertyNames) {
            String propertyValue = properties.getProperty(propertyName);
            if (propertyValue != null) {
                System.setProperty(propertyName, propertyValue);
            }
        }
    }

    public static Properties getProperties() {
        return new Properties(properties);
    }

    public static String getProperty(String key, boolean mustHave) {
        // System properties overwrite .jpy properties
        String property = System.getProperty(key);
        if (property != null) {
            return property;
        }
        property = properties.getProperty(key);
        if (property == null && mustHave) {
            throw new RuntimeException("missing configuration property '" + key + "'");
        }
        return property;
    }

    public static OS getOS() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return OS.WINDOWS;
        } else if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
            return OS.UNIX;
        } else if (os.contains("mac")) {
            return OS.MAC_OS;
        } else if (os.contains("sunos")) {
            return OS.SUNOS;
        }
        return null;
    }
}
