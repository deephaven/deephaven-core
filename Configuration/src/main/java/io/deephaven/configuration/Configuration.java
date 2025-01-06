//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.configuration;

import io.deephaven.internal.log.Bootstrap;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Utility class to provide an enhanced view and common access point for java properties files, as well as common
 * configuration pieces.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class Configuration extends PropertyFile {
    public static final String QUIET_PROPERTY = "configuration.quiet";

    private static final Logger log = LoggerFactory.getLogger(Configuration.class);

    private static Configuration DEFAULT;
    private static final Map<String, Configuration> NAMED_CONFIGURATIONS = new ConcurrentHashMap<>();
    private final Supplier<ConfigurationContext> contextSupplier;

    // This should never be null to meet the contract for getContextKeyValues()
    private Collection<String> contextKeys = Collections.emptySet();

    /**
     * The default Configuration implementation, which loads properties from the default property file in addition to
     * System properties.
     */
    private static class Default extends Configuration {
        Default(@NotNull final Supplier<ConfigurationContext> contextSupplier) {
            super(contextSupplier);
            init();
        }

        @Override
        protected String determinePropertyFile() {
            return ConfigDir.configurationFile();
        }

        @SuppressWarnings("unused")
        public String getPropertyNullable(String propertyName) {
            return properties.getProperty(propertyName);
        }
    }

    /**
     * A Named configuration that loads values from the file defined by the property `Configuration.name.rootFile`.
     */
    private static class Named extends Configuration {
        private final String name;

        Named(@NotNull final String name, @NotNull final Supplier<ConfigurationContext> contextSupplier) {
            super(contextSupplier);
            this.name = name;
            init();
        }

        @Override
        protected String determinePropertyFile() {
            final String propFile = System.getProperty("Configuration." + name + ".rootFile");
            if (propFile == null) {
                throw new ConfigurationException("No property file defined for named configuration " + name);
            }

            return propFile;
        }
    }

    /**
     * Get the default Configuration instance.
     *
     * @return the single instance of Configuration allowed in an application
     */
    public static Configuration getInstance() {
        if (DEFAULT == null) {
            DEFAULT = new Default(DefaultConfigurationContext::new);
        }
        return DEFAULT;
    }

    /**
     * Get the {@link Configuration} for the specified name.
     *
     * @param name the name of the configuration to load
     * @return the named configuration.
     * @throws ConfigurationException if the named configuration could not be loaded.
     */
    public static Configuration getNamed(@NotNull final String name) throws ConfigurationException {
        return NAMED_CONFIGURATIONS.computeIfAbsent(name, (k) -> {
            try {
                return new Named(name, DefaultConfigurationContext::new);
            } catch (ConfigurationException ex) {
                throw new ConfigurationException("Unable to load named configuration " + name, ex);
            }
        });
    }

    /**
     * Clear all currently loaded Configurations so that they may be loaded anew.
     */
    public static void reset() {
        DEFAULT = null;
        NAMED_CONFIGURATIONS.clear();
    }

    /**
     * Clear the specified named Configuration so it may be loaded anew.
     *
     * @param name the Configuration to clear.
     */
    public static void reset(@NotNull final String name) {
        NAMED_CONFIGURATIONS.remove(name);
    }

    /**
     * Create a new, non-cached, default Configuration instance.
     *
     * @return a new Configuration instance, guaranteed to not be cached.
     */
    public static Configuration newStandaloneConfiguration() {
        return newStandaloneConfiguration(DefaultConfigurationContext::new);
    }

    /**
     * Create a new, non-cached, default Configuration instance.
     *
     * @param contextSupplier the supplier for {@link ConfigurationContext}
     * @return a new Configuration instance, guaranteed to not be cached.
     */
    public static Configuration newStandaloneConfiguration(
            @NotNull final Supplier<ConfigurationContext> contextSupplier) {
        return new Default(contextSupplier);
    }

    /**
     * Create a new, non-cached, named Configuration instance.
     *
     * @param name the configuration name
     * @return a new Configuration instance, guaranteed to not be cached.
     */
    public static Configuration newStandaloneConfiguration(@NotNull final String name) {
        return newStandaloneConfiguration(name, DefaultConfigurationContext::new);
    }

    /**
     * Create a new, non-cached, named Configuration instance.
     *
     * @param name the configuration name
     * @param contextSupplier the supplier for {@link ConfigurationContext}
     * @return a new Configuration instance, guaranteed to not be cached.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static Configuration newStandaloneConfiguration(
            @NotNull final String name,
            @NotNull final Supplier<ConfigurationContext> contextSupplier) {
        return new Named(name, contextSupplier);
    }

    protected Configuration(@NotNull final Supplier<ConfigurationContext> contextSupplier) {
        this.contextSupplier = contextSupplier;
    }

    /**
     * Initialize the configuration. This will be sensitive to any system properties that configure the property file
     * path.
     */
    protected void init() {
        final String configurationFile;
        try {
            configurationFile = reloadProperties();
        } catch (IOException x) {
            throw new ConfigurationException("Could not process configuration from file", x);
        }

        // The quiet property is available because things like shell scripts may be parsing our System.out and they
        // don't want to have to deal with these log messages
        if (!isQuiet()) {
            log.info().append("Configuration: configuration file is ").append(configurationFile).endl();
        }
    }

    /**
     * Recursively load properties files allowing for overrides.
     *
     * @param fileName relative classpath path to the prop file to load
     * @param ignoreScope true if scope should be ignored when parsing the file, false otherwise.
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    private void load(String fileName, boolean ignoreScope) throws IOException, ConfigurationException {
        final ParsedProperties temp = new ParsedProperties(ignoreScope, contextSupplier.get());
        // we explicitly want to set 'properties' here so that if we get an error while loading, anything before that
        // error shows up.
        // That is very helpful in debugging.
        properties = temp;
        temp.load(fileName);
        contextKeys = temp.getContextKeyValues();
    }

    /**
     * Return the configuration contexts for this process. This is the list of properties that may have been used to
     * parse the configuration file. If the configuration has not been parsed, this collection may be empty. This
     * collection will be immutable.
     *
     * @return the configuration contexts.
     */
    @NotNull
    public Collection<String> getContextKeyValues() {
        return contextKeys;
    }

    /**
     * Treat the system property propertyName as a path, and perform substitution with {@link #expandLinuxPath(String)}.
     *
     * @param propertyName system property containing a path
     * @return The value of property propertyName after the manipulations.
     */
    public String lookupPath(String propertyName) {
        String result = System.getProperty(propertyName); // In case it's been set with System.setProperty after the
                                                          // Configuration instance was created
        if (result == null) {
            result = getStringWithDefault(propertyName, null);
        }
        if (result == null) {
            throw new ConfigurationException(propertyName + " property is not defined");
        }
        return expandLinuxPath(result);
    }

    /**
     * Expand the Linux-style path.
     * <ul>
     * <li>Change linux-style absolute paths to platform independent absolute. If the path starts with "/", replace "/"
     * with the current directory's root (e.g. "C:\" on Windows.</li>
     * <li>If the path begins with "~/", then replace the ~ with the user.home system property.</li>
     * <li>If the path does not begin with "~/", then replace all occurrences of ~ with system property user.name.</li>
     * <li>Make sure the path ends in File.separator.</li>
     * </ul>
     *
     * @param path the path to be adjusted
     * @return the path with substitutions performed
     */
    @SuppressWarnings("WeakerAccess")
    public static String expandLinuxPath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
            path = getRootPath() + path;
        }
        if (path.startsWith("~/")) {
            path = path.substring(1);
            path = System.getProperty("user.home") + path;
        } else {
            // expand ~ to user name if not in the beginning
            int index = path.indexOf("~");
            if (index != -1)
                path = path.replaceAll("~", System.getProperty("user.name"));
        }
        if (!path.endsWith(File.separator)) {
            path += File.separator;
        }
        return path;
    }

    /**
     * Get the prefix for absolute files on this system. For example, "/" on linux and "C:\" on Windows.
     *
     * @return The absolute path prefix.
     */
    private static String getRootPath() {
        File dir = new File(".");
        dir = dir.getAbsoluteFile();
        File parent = dir.getParentFile();
        while (parent != null) {
            dir = parent;
            parent = dir.getParentFile();
        }
        return dir.getAbsolutePath();
    }

    /**
     * @return the TimeZone the server is running in
     */
    public TimeZone getServerTimezone() {
        return TimeZone.getTimeZone(ZoneId.systemDefault());
    }

    /**
     * Reload properties, then update with all system properties (properties set in System take precedence).
     *
     * @return the property file used
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    public String reloadProperties() throws IOException, ConfigurationException {
        return reloadProperties(false);
    }

    protected abstract String determinePropertyFile();

    /**
     * Reload properties, optionally ignoring scope sections - used for testing
     *
     * @param ignoreScope True if scope declarations in the property file should be ignored, false otherwise. Used only
     *        for testing.
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    String reloadProperties(boolean ignoreScope) throws IOException, ConfigurationException {
        final String propertyFile = determinePropertyFile();
        load(propertyFile, ignoreScope);
        // If any system properties exist with the same name as a property that's been declared final, that will
        // generate an exception the same way it would inside the properties file.
        properties.putAll(System.getProperties());
        return propertyFile;
    }

    // only used by main() method below, normally configs are loaded from the classpath
    private Properties load(String path, String propFileName) throws IOException {
        final Properties temp = new ParsedProperties();
        temp.load(new FileInputStream(path + "/" + propFileName));
        return temp;
    }

    /**
     * The following main method compares two directories of prop files and outputs a CSV report of the differences.
     * Usually run before the release of a new version into prod
     *
     * @param args dir1 dir2 outFile.csv
     */
    public static void main(String[] args) {
        // property dump
        try {
            if (args.length < 3) {
                System.err.println("Usage: Configuration oldEtcPath newEtcPath outCSV");
                System.exit(1);
            }

            String oldEtcPath = args[0];
            String newEtcPath = args[1];
            PrintWriter out = new PrintWriter(new File(args[2]));

            HashSet<String> diffSet = new HashSet<>();
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-prod.prop", newEtcPath, "ise-prod.prop", "", "",
                    false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-stage.prop", newEtcPath, "ise-stage.prop", "", "",
                    false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-simulation.prop", newEtcPath, "ise-simulation.prop",
                    "", "", false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, newEtcPath, "ise-prod.prop", newEtcPath, "ise-stage.prop", newEtcPath,
                    "ise-simulation.prop", true));
            out.print('\n');
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String propFileDiffReport(Set<String> includedProperties, String dir1, String file1, String dir2,
            String file2, String dir3, String file3, boolean useDiffKeys) throws IOException {
        StringBuilder out = new StringBuilder();
        Configuration configuration = new Default(DefaultConfigurationContext::new);
        Properties leftProperties = configuration.load(dir1, file1);
        Properties rightProperties = configuration.load(dir2, file2);
        Properties right2Properties;

        if (dir3.length() > 0) {
            right2Properties = configuration.load(dir3, file3);
        } else {
            right2Properties = new Properties();
        }

        Set<String> keynames = new TreeSet<>();

        for (Enumeration<?> enumeration = leftProperties.propertyNames(); enumeration.hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }
        for (Enumeration<?> enumeration = rightProperties.propertyNames(); enumeration.hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }
        for (Enumeration<?> enumeration = right2Properties.propertyNames(); enumeration.hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }

        out.append("key,").append(dir1).append(File.separator).append(file1).append(",").append(dir2)
                .append(File.separator).append(file2).append(",").append(dir3).append(File.separator).append(file3)
                .append("\n");
        for (String sKey : keynames) {
            String sLeftValue = leftProperties.containsKey(sKey) ? leftProperties.getProperty(sKey) : "";
            String sRightValue = rightProperties.containsKey(sKey) ? rightProperties.getProperty(sKey) : "";
            String sRightValue2 = right2Properties.containsKey(sKey) ? right2Properties.getProperty(sKey) : "";
            boolean bSame;
            if (dir3.length() > 0) {
                bSame = sLeftValue.equals(sRightValue) && sLeftValue.equals(sRightValue2)
                        && sRightValue.equals(sRightValue2);
            } else {
                bSame = sLeftValue.equals(sRightValue);
            }
            if (!bSame) {
                if (useDiffKeys) {
                    if (includedProperties.contains(sKey)) {
                        writeLine(out, sKey, sLeftValue, sRightValue, sRightValue2);
                    }
                } else {
                    includedProperties.add(sKey);
                    writeLine(out, sKey, sLeftValue, sRightValue, sRightValue2);
                }
            }
        }
        return out.toString();
    }

    private static void writeLine(StringBuilder out, String sKey, String sLeftValue, String sRightValue,
            String sRightValue2) {
        out.append(sKey).append(", \"").append(sLeftValue).append("\", \"").append(sRightValue).append("\", \"")
                .append(sRightValue2).append("\"\n");
    }

    static boolean isQuiet() {
        return Bootstrap.isQuiet() || System.getProperty(QUIET_PROPERTY) != null;
    }
}
