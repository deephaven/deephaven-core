/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.configuration;

import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.*;

/**
 * Utility class to provide an enhanced view and common access point for java properties files, as well as common
 * configuration pieces.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class Configuration extends PropertyFile {

    /** Property that specifies the workspace. */
    @SuppressWarnings("WeakerAccess")
    public static final String WORKSPACE_PROPERTY = "workspace";

    /** Token used for process name substitution */
    private static final String PROCESS_NAME_TOKEN = "<processname>";

    /** Token used for workspace substitution */
    private static final String WORKSPACE_TOKEN = "<workspace>";

    /** Ordered list of properties that can specify the configuration root file */
    @SuppressWarnings("WeakerAccess")
    static final String[] FILE_NAME_PROPERTIES = {"Configuration.rootFile"};
    public static final String QUIET_PROPERTY = "configuration.quiet";
    private static final String PROCESS_NAME_PROPERTY = "process.name";

    private static NullableConfiguration INSTANCE = null;

    private static final Logger log = LoggerFactory.getLogger(Configuration.class);

    private final String confFileName;
    private final String confFileProperty;
    private final String workspace;

    // This should never be null to meet the contract for getContextKeyValues()
    private Collection<String> contextKeys = Collections.emptySet();

    /**
     * Get the default Configuration instance.
     *
     * @return the single instance of Configuration allowed in an application
     */
    public static Configuration getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new NullableConfiguration();
        }
        return INSTANCE;
    }

    /**
     * Get the process name based on the standard process name property {@link #PROCESS_NAME_PROPERTY}. Throw an
     * exception if the property name does not exist.
     *
     * @return the process name
     */
    public String getProcessName() {
        return getProcessName(true);
    }

    /**
     * Get the process name based on the standard process name property {@link #PROCESS_NAME_PROPERTY}. If the property
     * does not exist and requireProcessName is true, throw an exception. If the property does not exist and
     * requireProcessName is false, return null.
     *
     * @param requireProcessName if true, throw an exception if the process name can't be found or is empty
     * @return the process name, or null if the process name can't be determined and requireProcessName is false
     */
    @SuppressWarnings("WeakerAccess")
    public @Nullable String getProcessName(final boolean requireProcessName) {
        final String processName = getStringWithDefault(PROCESS_NAME_PROPERTY, null);
        if (requireProcessName && (processName == null || processName.isEmpty())) {
            throw new ConfigurationException("Property " + PROCESS_NAME_PROPERTY + " must be defined and non-empty");
        }
        return processName;
    }

    /**
     * Clear the current instance, so the next call to getInstance() gives us a new one
     */
    public static void reset() {
        INSTANCE = null;
    }

    @SuppressWarnings("UnusedReturnValue")
    public static Configuration TEST_NEW_Configuration() {
        return new Configuration();
    }

    /**
     * Find the name of the property specifying the root configuration file. The first property set in the ordered list
     * of candidates is returned, or null if none is set.
     * 
     * @see #FILE_NAME_PROPERTIES
     *
     * @return the name of the property specifying a configuration file, or NULL if none is set
     */
    @SuppressWarnings("WeakerAccess")
    public static String determineConfFileProperty() {
        for (final String propertyName : FILE_NAME_PROPERTIES) {
            final String fileName = System.getProperty(propertyName);
            if (fileName != null) {
                return propertyName;
            }
        }
        return null;
    }

    /**
     * If one of the valid configuration file properties is set, return the value, else return NULL
     * 
     * @see #FILE_NAME_PROPERTIES
     *
     * @return the configuration file value, or NULL if no valid property is set.
     */
    public static String getConfFileNameFromProperties() {
        final String property = determineConfFileProperty();
        return property == null ? null : System.getProperty(property);
    }

    protected Configuration() {
        confFileProperty = determineConfFileProperty();
        if (confFileProperty == null) {
            log.error("property " + FILE_NAME_PROPERTIES[0] + " must be specified");
            System.exit(-1);
        }
        confFileName = System.getProperty(confFileProperty);

        try {
            reloadProperties();
        } catch (IOException x) {
            throw new ConfigurationException(
                    "Could not process configuration from file " + confFileName + " in CLASSPATH.", x);
        }

        String workspacePropValue;
        try {
            workspacePropValue = lookupPath(WORKSPACE_PROPERTY);
        } catch (ConfigurationException e) {
            workspacePropValue = null;
        }
        workspace = workspacePropValue;

        // The quiet property is available because things like shell scripts may be parsing our System.out and they
        // don't
        // want to have to deal with these log messages
        if (System.getProperty(QUIET_PROPERTY) == null) {
            if (workspace != null) {
                log.info("Configuration: " + WORKSPACE_PROPERTY + " is " + workspace);
            } else {
                log.warn("Configuration: " + WORKSPACE_PROPERTY + " is undefined");
            }
            log.info("Configuration: " + confFileProperty + " is " + confFileName);
        }
    }

    String getConfFileProperty() {
        return confFileProperty;
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
        final ParsedProperties temp = new ParsedProperties(ignoreScope);
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

    @SuppressWarnings("NeverUsed")
    public String getConfFileName() {
        return confFileName;
    }

    private void checkWorkspaceDefined() {
        if (workspace == null) {
            throw new ConfigurationException(WORKSPACE_PROPERTY + " property is not defined");
        }
    }

    public String getWorkspacePath(String propertyName) {
        checkWorkspaceDefined();
        return workspace + getProperty(propertyName);
    }

    public String getWorkspacePath() {
        checkWorkspaceDefined();
        return workspace;
    }

    @SuppressWarnings("unused")
    public String getTempPath(String componentName) {
        File f = new File(getWorkspacePath() + "/temp/" + componentName);
        if (!f.exists()) {
            // noinspection ResultOfMethodCallIgnored
            f.mkdirs();
        }
        return f.getAbsolutePath();
    }

    /**
     * @return the TimeZone the server is running in
     */
    public TimeZone getServerTimezone() {
        return TimeZone.getTimeZone(getProperty("server.timezone"));
    }

    /**
     * Reload properties, then update with all system properties (properties set in System take precedence).
     *
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    public void reloadProperties() throws IOException, ConfigurationException {
        reloadProperties(false);
    }

    /**
     * Reload properties, optionally ignoring scope sections - used for testing
     *
     * @param ignoreScope True if scope declarations in the property file should be ignored, false otherwise. Used only
     *        for testing.
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    void reloadProperties(boolean ignoreScope) throws IOException, ConfigurationException {
        load(System.getProperty(confFileProperty), ignoreScope);
        // If any system properties exist with the same name as a property that's been declared final, that will
        // generate
        // an exception the same way it would inside the properties file.
        properties.putAll(System.getProperties());
    }

    /**
     * ONLY the service factory is allowed to get null properties and ONLY for the purposes of using default profiles
     * when one doesn't exist. This has been relocated here after many people are using defaults/nulls in the code when
     * it's not allowed.
     */
    @SuppressWarnings("WeakerAccess")
    public static class NullableConfiguration extends Configuration {
        NullableConfiguration() {
            super();
        }

        @SuppressWarnings("unused")
        public String getPropertyNullable(String propertyName) {
            return properties.getProperty(propertyName);
        }
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
        Configuration configuration = new Configuration();
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
}
