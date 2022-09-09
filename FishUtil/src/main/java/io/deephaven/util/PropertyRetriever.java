/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.ConfigurationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.file.Files;

/**
 * Class to assist with retrieving properties such as passwords from environment variables, files, and properties.
 */
public class PropertyRetriever {

    /**
     * Return a property value from a set of possible locations, allowing for optional base64 decoding. The following
     * order is used.
     * <ul>
     * <li>First, if an environment variable is provided, it is checked. if it exists, then it is returned,
     * base64-decoded if requested.</li>
     * <li>Next, the property file is checked if the property is provided (the configuration is checked for the provided
     * property, and the resulting property value defines the filename). If it exists, the contents are read,
     * base64-decoded if requested, then returned.</li>
     * <li>Finally, the property is checked. If it exists it is base64-decoded if requested and returned.</li>
     * <li>At least one of environmentVariable, fileProperty, or propertyName must be specified and exist.</li>
     * <li>If both a property file and property name are provided and exist in the Configuration instance, an exception
     * will be thrown.</li>
     * </ul>
     *
     * @param configuration the Configuration instance to check
     * @param propertyMeaning a user-friendly property meaning, included in thrown exceptions
     * @param environmentVariable an optional environment variable to check for the value
     * @param fileProperty an optional Configuration property that specifies the file that contains the value
     * @param propertyName an optional Configuration property that specifies the value
     * @param base64Encoded if true, the retrieved value is base64 decoded before being returned to the caller
     * @return the found value, base64-decoded if requested
     */
    public static String getProperty(@NotNull final Configuration configuration,
            @NotNull final String propertyMeaning,
            @Nullable final String environmentVariable,
            @Nullable final String fileProperty,
            @Nullable final String propertyName,
            final boolean base64Encoded) {
        // This means a coding error on the caller's part
        if (environmentVariable == null && fileProperty == null && propertyName == null) {
            throw new ConfigurationException(
                    "No environment variable or properties defined to retrieve property for " + propertyMeaning);
        }

        // The environment variable takes precedence
        String propertyValue = getPropertyFromEnvironmentVariable(environmentVariable);

        // If nothing was retrieved from the environment variable, then check the property and file
        if (propertyValue == null) {
            propertyValue = getPropertyFromFileOrProperty(configuration, propertyMeaning, fileProperty, propertyName);
        }

        // If it's still null nothing could be found
        if (propertyValue == null) {
            final StringBuilder propertyPossibilities = new StringBuilder();
            if (environmentVariable != null) {
                propertyPossibilities.append("environment variable ").append(environmentVariable);
            }

            if (fileProperty != null) {
                propertyPossibilities
                        .append(propertyPossibilities.length() == 0 ? "filename property " : " or filename property ");
                propertyPossibilities.append(fileProperty);
            }
            if (propertyName != null) {
                propertyPossibilities.append(propertyPossibilities.length() == 0 ? "property " : " or property ");
                propertyPossibilities.append(propertyName);
            }
            throw new ConfigurationException(
                    "No " + propertyMeaning + " set, please set " + propertyPossibilities.toString());
        }

        if (base64Encoded) {
            return new String(org.apache.commons.codec.binary.Base64.decodeBase64(propertyValue));
        } else {
            return propertyValue;
        }
    }

    private static String getPropertyFromEnvironmentVariable(final String environmentVariable) {
        return environmentVariable != null ? System.getenv(environmentVariable) : null;
    }

    private static String getPropertyFromFileOrProperty(
            final Configuration configuration, final String propertyMeaning,
            final String fileProperty, final String propertyName) {
        if (fileProperty != null && configuration.hasProperty(fileProperty)) {
            if (propertyName != null && configuration.hasProperty(propertyName)) {
                throw new IllegalArgumentException("Conflicting properties for " + propertyMeaning + " - both "
                        + fileProperty + " and " + propertyName + " are set.");
            }
            final String propertyFilename = configuration.getProperty(fileProperty);
            final File propertyFile = new File(propertyFilename);
            try {
                return Files.readString(propertyFile.toPath()).trim();
            } catch (IOException e) {
                try (InputStream resourceAsStream =
                        PropertyRetriever.class.getResourceAsStream("/" + propertyFilename)) {
                    if (resourceAsStream == null) {
                        throw new ConfigurationException("Unable to open file " + propertyFilename
                                + " specified by " + fileProperty + " for " + propertyMeaning);
                    }
                    final BufferedReader bufferedReader =
                            new BufferedReader(new InputStreamReader(resourceAsStream));
                    return bufferedReader.readLine();
                } catch (IOException e2) {
                    throw new UncheckedIOException(
                            "Can not read property file " + propertyFilename + " for " + propertyMeaning, e2);
                }
            }
        } else if (propertyName != null && configuration.hasProperty(propertyName)) {
            return configuration.getProperty(propertyName);
        }
        return null;
    }
}
