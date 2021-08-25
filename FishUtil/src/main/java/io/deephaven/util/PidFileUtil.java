/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.configuration.Configuration;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * Utility for manipulating PID files.
 */
@SuppressWarnings("WeakerAccess")
public class PidFileUtil {

    private static final String FILE_SUFFIX = ".pid";
    private static final String PID_FILE_DIRECTORY_PROPERTY = "pidFileDirectory";

    /**
     * Atomically create a new file, and then write this process' PID to it.
     *
     * @param configuration The configuration to use for property lookup
     * @throws IllegalStateException If pidFile exists or cannot be created/opened
     */
    public static void checkAndCreatePidFileForThisProcess(@NotNull final Configuration configuration) {
        checkAndCreatePidFileForProcessName(configuration, configuration.getProcessName());
    }

    /**
     * Atomically create a new file, and then write this process' PID to it.
     *
     * @param configuration The configuration to use for property lookup
     * @param processName The name to be used for the per-process unique portion of the PID file's path
     * @throws IllegalStateException If pidFile exists or cannot be created/opened
     */
    public static void checkAndCreatePidFileForProcessName(@NotNull final Configuration configuration,
            @NotNull final String processName) {
        final String directoryName = configuration.getProperty(PID_FILE_DIRECTORY_PROPERTY);
        checkAndCreatePidFile(new File(directoryName, processName + FILE_SUFFIX));
    }

    private static String getProcessIdString() {
        if (OSUtil.runningLinux()) {
            return getProcessIdStringFromProcSelf();
        }
        return getProcessIdStringFromJvmName();
    }

    private static String getProcessIdStringFromProcSelf() {
        final File selfFile;
        try {
            selfFile = new File("/proc/self").getCanonicalFile();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to canonicalize /proc/self", e);
        }
        return selfFile.getName();
    }

    private static String getProcessIdStringFromJvmName() {
        final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        return jvmName.substring(0, jvmName.indexOf('@'));
    }

    /**
     * Atomically create a new file, and then write this process' PID to it.
     *
     * @param pidFile the file to be checked or created
     * @throws IllegalStateException If pidFile exists or cannot be created/opened
     */
    public static void checkAndCreatePidFile(final File pidFile) throws IllegalStateException {
        final String processIdString;
        try {
            processIdString = getProcessIdString();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to lookup process ID", e);
        }

        try {
            if (!pidFile.createNewFile()) {
                throw new IllegalStateException("Pid file " + pidFile
                        + " already exists - check running process and manually delete if necessary");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create pid file " + pidFile, e);
        }
        pidFile.deleteOnExit();

        final FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(pidFile);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open pid file " + pidFile + " for writing", e);
        }

        try {
            fileWriter.write(processIdString);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write pid to file " + pidFile, e);
        }

        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close pid file " + pidFile, e);
        }
        System.out.println("Created " + pidFile + " with PID " + processIdString);
    }
}
