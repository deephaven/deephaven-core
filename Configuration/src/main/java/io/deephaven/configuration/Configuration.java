/*
 * Copyright (c) 2016-2018 Deephaven and Patent Pending
 */

package io.deephaven.configuration;

import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Utility class to provide an enhanced view and common access point for java properties files, as
 * well as common configuration pieces such as log directories and workspace-related properties.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class Configuration extends PropertyFile {

    /** Property that specifies the workspace. */
    @SuppressWarnings("WeakerAccess")
    public static final String WORKSPACE_PROPERTY = "workspace";

    /** Property that specifies the software installation root directory. */
    @SuppressWarnings("WeakerAccess")
    public static final String DEVROOT_PROPERTY = "devroot";

    /** Property that specifies the directory for process logs. */
    @SuppressWarnings("WeakerAccess")
    public static final String LOGDIR_PROPERTY = "logDir"; // Defaults to
                                                           // getProperty(WORKSPACE_PROPERTY)/../logs

    /** Property that specifies the default process log directory. */
    private static final String LOGDIR_DEFAULT_PROPERTY = "defaultLogDir";

    /** Property that specifies a base directory for logging. */
    private static final String LOGDIR_ROOT_PROPERTY = "logroot";

    /** Property that specifies the DB root value */
    private static final String DB_ROOT_PROPERTY = "OnDiskDatabase.rootDirectory";

    /** Token used for log root directory substitution */
    private static final String LOGROOT_TOKEN = "<logroot>";

    /** Token used for process name substitution */
    private static final String PROCESS_NAME_TOKEN = "<processname>";

    /** Token used for devroot substitution */
    private static final String DEVROOT_TOKEN = "<devroot>";

    /** Token used for workspace substitution */
    private static final String WORKSPACE_TOKEN = "<workspace>";

    /** Token used for db root directory substitution */
    private static final String DB_ROOT_TOKEN = "<dbroot>";

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
    private final String devroot;

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
     * Get the process name based on the standard process name property
     * {@link #PROCESS_NAME_PROPERTY}. Throw an exception if the property name does not exist.
     *
     * @return the process name
     */
    public String getProcessName() {
        return getProcessName(true);
    }

    /**
     * Get the process name based on the standard process name property
     * {@link #PROCESS_NAME_PROPERTY}. If the property does not exist and requireProcessName is
     * true, throw an exception. If the property does not exist and requireProcessName is false,
     * return null.
     *
     * @param requireProcessName if true, throw an exception if the process name can't be found or
     *        is empty
     * @return the process name, or null if the process name can't be determined and
     *         requireProcessName is false
     */
    @SuppressWarnings("WeakerAccess")
    public @Nullable String getProcessName(final boolean requireProcessName) {
        final String processName = getStringWithDefault(PROCESS_NAME_PROPERTY, null);
        if (requireProcessName && (processName == null || processName.isEmpty())) {
            throw new ConfigurationException(
                "Property " + PROCESS_NAME_PROPERTY + " must be defined and non-empty");
        }
        return processName;
    }

    /**
     * Determine the directory where process logs should be written. This is based off a series of
     * cascading properties.
     * <ul>
     * <li>If the system property {@link #LOGDIR_PROPERTY} is set, the assumption is that a JVM
     * parameter has been defined and it takes precedence</li>
     * <li>Otherwise, if the process name is defined, the properties
     * {@link #LOGDIR_PROPERTY}.processName is used</li>
     * <li>If no value has been determined, the default log directory property
     * {@link #LOGDIR_DEFAULT_PROPERTY} is used</li>
     * <li>If no value has been determined, "{@link #WORKSPACE_PROPERTY}/../logs" is used</li>
     * </ul>
     * In all cases except the first (where {@link #LOGDIR_PROPERTY} is defined with the system
     * property), the directory is normalized with {@link #normalizeLogDirectoryPath(String)}. If
     * validateOrCreateDirectory is true, then if the filesystem contains the determined directory,
     * it is validated to be a directory, or if it does not exit it is created.
     *
     * @param validateOrCreateDirectory if true, then if the directory exists validate that it is a
     *        directory, and create it if it doesn't exist
     * @return the directory where process logs should be written
     */
    public String getLogDir(final boolean validateOrCreateDirectory) {
        final String logDirJvmProp = getStringWithDefault(LOGDIR_PROPERTY, null);
        if (logDirJvmProp != null) {
            return logDirJvmProp;
        }

        // Otherwise use the full property lookup logic
        String processName = getInstance().getProcessName(false);
        String logDir;
        if (processName != null && !processName.isEmpty()) {
            logDir = getPossibleStringWithDefault(null,
                LOGDIR_PROPERTY + "." + processName,
                LOGDIR_DEFAULT_PROPERTY);
        } else {
            logDir = getStringWithDefault(LOGDIR_DEFAULT_PROPERTY, null);
        }

        if (logDir == null || logDir.isEmpty()) {
            logDir = lookupPath(WORKSPACE_PROPERTY) + "../logs";
        }

        final String normalizedDirectory = normalizeLogDirectoryPath(logDir);
        if (!validateOrCreateDirectory) {
            return normalizedDirectory;
        }

        try {
            checkDirectory(normalizedDirectory, true, "Process log directory");
        } catch (IOException e) {
            throw new UncheckedIOException("Error validating directory " + normalizedDirectory, e);
        }
        return normalizedDirectory;
    }

    /**
     * Determine the directory where process logs should be written using
     * {@link #getLogDir(boolean)}, validating that the directory exists or creating the directory
     * if it doesn't exist.
     *
     * @return the directory where process logs should be written
     */
    public String getLogDir() {
        return getLogDir(true);
    }

    /**
     * Normalize a directory path. This performs the following substitutions and manipulations.
     * <ul>
     * <li>{@code <dbroot>} - replaced with the on disk database root directory, usually /db</li>
     * <li>{@code <workspace>} - replaced with the process workspace</li>
     * <li>{@code <devroot>} - replaced with the installation root directory</li>
     * <li>{@code <processname>} - replaced with the process name</li>
     * <li>{@code <logroot>} - replaced with the value found by the property
     * {@link #LOGDIR_ROOT_PROPERTY}</li>
     * <li>After all substitutions, {@link #expandLinuxPath(String)} is called</li>
     * <li>Finally, {@link Path#normalize()} is called</li>
     * </ul>
     *
     * @param directoryName the directory name to be normalized
     * @return the normalized directory path after the substitutions have been performed
     */
    public String normalizeLogDirectoryPath(@NotNull final String directoryName) {
        String substitutedPath;
        // First see if there's a root to use as a prefix
        if (directoryName.contains(LOGROOT_TOKEN)) {
            final String logDirRoot = getStringWithDefault(LOGDIR_ROOT_PROPERTY, null);
            if (logDirRoot == null) {
                throw new IllegalArgumentException("Directory " + directoryName + " contains "
                    + LOGROOT_TOKEN + " but " + LOGDIR_ROOT_PROPERTY + " property is not defined");
            }
            substitutedPath = directoryName.replace("<logroot>", logDirRoot);
        } else {
            substitutedPath = directoryName;
        }

        if (substitutedPath.contains(PROCESS_NAME_TOKEN)) {
            final String processName = getStringWithDefault(PROCESS_NAME_PROPERTY, null);
            if (processName == null) {
                throw new IllegalArgumentException("Directory " + substitutedPath
                    + " (original path " + directoryName + ") contains " + PROCESS_NAME_TOKEN
                    + " but " + PROCESS_NAME_PROPERTY + " property is not defined");
            } else {
                substitutedPath = substitutedPath.replace(PROCESS_NAME_TOKEN, processName);
            }
        }

        if (substitutedPath.contains(WORKSPACE_TOKEN)) {
            if (workspace == null) {
                throw new IllegalArgumentException("Directory " + substitutedPath
                    + " (original path " + directoryName + ") contains " + WORKSPACE_TOKEN + " but "
                    + WORKSPACE_PROPERTY + " property is not defined");
            } else {
                substitutedPath = substitutedPath.replace(WORKSPACE_TOKEN, workspace);
            }
        }

        if (substitutedPath.contains(DEVROOT_TOKEN)) {
            if (devroot == null) {
                throw new IllegalArgumentException("Directory " + substitutedPath
                    + " (original path " + directoryName + ") contains " + DEVROOT_TOKEN + " but "
                    + DEVROOT_PROPERTY + " property is not defined");
            } else {
                substitutedPath = substitutedPath.replace(DEVROOT_TOKEN, devroot);
            }
        }

        if (substitutedPath.contains(DB_ROOT_TOKEN)) {
            final String dbRoot = getStringWithDefault(DB_ROOT_PROPERTY, null);
            if (dbRoot == null) {
                throw new IllegalArgumentException("Directory " + substitutedPath
                    + " (original path " + directoryName + ") contains " + DB_ROOT_TOKEN + " but "
                    + DB_ROOT_PROPERTY + " property is not defined");
            }
            substitutedPath = substitutedPath.replace(DB_ROOT_TOKEN,
                getStringWithDefault(DB_ROOT_PROPERTY, null));
        }

        // Now perform the expansion of any linux-like (i.e. ~) path pieces
        final String expandedPath = expandLinuxPath(substitutedPath);

        final Path path = Paths.get(expandedPath);
        return path.normalize().toString();
    }

    public void checkDirectory(final String dir, final boolean createDirectory,
        final String message) throws IOException {
        final File logDirFile = new File(dir);
        if (!logDirFile.exists()) {
            if (!createDirectory) {
                throw new IOException(
                    message + " " + dir + " does not exist and createDirectory=false");
            }

            final boolean dirCreated;
            try {
                dirCreated = logDirFile.mkdirs();
            } catch (SecurityException e) {
                throw new IOException(message + " " + dir + " could not be created: ", e);
            }
            if (!dirCreated) {
                throw new IOException(message + " " + dir + " could not be created");
            }
        } else if (!logDirFile.isDirectory()) {
            throw new IllegalArgumentException(
                message + " " + dir + " exists but is not a directory");
        }
    }

    /**
     * Compute the log dir and filename for the provided log file name.
     *
     * @param filename the name of the log file
     * @return the full path and filename for the given filename
     */
    public String getLogPath(String filename) {
        return getLogDir() + File.separator + filename;
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
     * Find the name of the property specifying the root configuration file. The first property set
     * in the ordered list of candidates is returned, or null if none is set.
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

        String devrootPropValue;
        try {
            devrootPropValue = lookupPath(DEVROOT_PROPERTY);
        } catch (ConfigurationException e) {
            devrootPropValue = null;
        }
        devroot = devrootPropValue;

        // The quiet property is available because things like shell scripts may be parsing our
        // System.out and they don't
        // want to have to deal with these log messages
        if (System.getProperty(QUIET_PROPERTY) == null) {
            if (workspace != null) {
                log.info("Configuration: " + WORKSPACE_PROPERTY + " is " + workspace);
            } else {
                log.warn("Configuration: " + WORKSPACE_PROPERTY + " is undefined");
            }
            if (devroot != null) {
                log.info("Configuration: " + DEVROOT_PROPERTY + " is " + devroot);
            } else {
                log.warn("Configuration: " + DEVROOT_PROPERTY + " is undefined");
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
    private void load(String fileName, boolean ignoreScope)
        throws IOException, ConfigurationException {
        final ParsedProperties temp = new ParsedProperties(ignoreScope);
        // we explicitly want to set 'properties' here so that if we get an error while loading,
        // anything before that error shows up.
        // That is very helpful in debugging.
        properties = temp;
        temp.load(fileName);
        contextKeys = temp.getContextKeyValues();
    }

    /**
     * Return the configuration contexts for this process. This is the list of properties that may
     * have been used to parse the configuration file. If the configuration has not been parsed,
     * this collection may be empty. This collection will be immutable.
     *
     * @return the configuration contexts.
     */
    @NotNull
    public Collection<String> getContextKeyValues() {
        return contextKeys;
    }

    /**
     * Treat the system property propertyName as a path, and perform substitution with
     * {@link #expandLinuxPath(String)}.
     *
     * @param propertyName system property containing a path
     * @return The value of property propertyName after the manipulations.
     */
    public String lookupPath(String propertyName) {
        String result = System.getProperty(propertyName); // In case it's been set with
                                                          // System.setProperty after the
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
     * <li>Change linux-style absolute paths to platform independent absolute. If the path starts
     * with "/", replace "/" with the current directory's root (e.g. "C:\" on Windows.</li>
     * <li>If the path begins with "~/", then replace the ~ with the user.home system property.</li>
     * <li>If the path does not begin with "~/", then replace all occurrences of ~ with system
     * property user.name.</li>
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
     * Get the prefix for absolute files on this system. For example, "/" on linux and "C:\" on
     * Windows.
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

    private void checkDevRootDefined() {
        if (devroot == null) {
            throw new ConfigurationException(DEVROOT_PROPERTY + " property is not defined");
        }
    }

    public String getDevRootPath(String propertyName) {
        checkDevRootDefined();
        return devroot + File.separator + getProperty(propertyName);
    }

    public String getDevRootPath() {
        checkDevRootDefined();
        return devroot;
    }

    /**
     * @return the TimeZone the server is running in
     */
    public TimeZone getServerTimezone() {
        return TimeZone.getTimeZone(getProperty("server.timezone"));
    }

    /**
     * Reload properties, then update with all system properties (properties set in System take
     * precedence).
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
     * @param ignoreScope True if scope declarations in the property file should be ignored, false
     *        otherwise. Used only for testing.
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     */
    void reloadProperties(boolean ignoreScope) throws IOException, ConfigurationException {
        load(System.getProperty(confFileProperty), ignoreScope);
        // If any system properties exist with the same name as a property that's been declared
        // final, that will generate
        // an exception the same way it would inside the properties file.
        properties.putAll(System.getProperties());
    }

    /**
     * ONLY the service factory is allowed to get null properties and ONLY for the purposes of using
     * default profiles when one doesn't exist. This has been relocated here after many people are
     * using defaults/nulls in the code when it's not allowed.
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
     * The following main method compares two directories of prop files and outputs a CSV report of
     * the differences. Usually run before the release of a new version into prod
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
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-prod.prop", newEtcPath,
                "ise-prod.prop", "", "", false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-stage.prop", newEtcPath,
                "ise-stage.prop", "", "", false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, oldEtcPath, "ise-simulation.prop", newEtcPath,
                "ise-simulation.prop", "", "", false));
            out.print('\n');
            out.print(propFileDiffReport(diffSet, newEtcPath, "ise-prod.prop", newEtcPath,
                "ise-stage.prop", newEtcPath, "ise-simulation.prop", true));
            out.print('\n');
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String propFileDiffReport(Set<String> includedProperties, String dir1,
        String file1, String dir2, String file2, String dir3, String file3, boolean useDiffKeys)
        throws IOException {
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

        for (Enumeration<?> enumeration = leftProperties.propertyNames(); enumeration
            .hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }
        for (Enumeration<?> enumeration = rightProperties.propertyNames(); enumeration
            .hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }
        for (Enumeration<?> enumeration = right2Properties.propertyNames(); enumeration
            .hasMoreElements();) {
            keynames.add((String) enumeration.nextElement());
        }

        out.append("key,").append(dir1).append(File.separator).append(file1).append(",")
            .append(dir2).append(File.separator).append(file2).append(",").append(dir3)
            .append(File.separator).append(file3).append("\n");
        for (String sKey : keynames) {
            String sLeftValue =
                leftProperties.containsKey(sKey) ? leftProperties.getProperty(sKey) : "";
            String sRightValue =
                rightProperties.containsKey(sKey) ? rightProperties.getProperty(sKey) : "";
            String sRightValue2 =
                right2Properties.containsKey(sKey) ? right2Properties.getProperty(sKey) : "";
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

    private static void writeLine(StringBuilder out, String sKey, String sLeftValue,
        String sRightValue, String sRightValue2) {
        out.append(sKey).append(", \"").append(sLeftValue).append("\", \"").append(sRightValue)
            .append("\", \"").append(sRightValue2).append("\"\n");
    }
}
