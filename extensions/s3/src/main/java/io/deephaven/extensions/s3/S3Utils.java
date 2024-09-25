//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileLocation;

import java.nio.file.Path;
import java.util.Optional;

class S3Utils {

    /**
     * Aggregates the profile files for configuration and credentials files into a single {@link ProfileFile}.
     *
     * @param configFilePathStr An {@link Optional} containing the path to the configuration file as a string.
     * @param credentialsFilePathStr An {@link Optional} containing the path to the credentials file as a string.
     *
     * @return A {@link ProfileFile} that aggregates the configuration and credentials files.
     */
    static ProfileFile aggregateProfileFile(
            @NotNull final Optional<String> configFilePathStr,
            @NotNull final Optional<String> credentialsFilePathStr) {
        final ProfileFile.Aggregator builder = ProfileFile.aggregator();

        // Add the configuration file
        final Optional<Path> configFilePath =
                configFilePathStr.map(Path::of).or(ProfileFileLocation::configurationFileLocation);
        configFilePath.ifPresent(path -> addProfileFile(builder, ProfileFile.Type.CONFIGURATION, path));

        // Add the credentials file
        final Optional<Path> credentialsFilePath =
                credentialsFilePathStr.map(Path::of).or(ProfileFileLocation::credentialsFileLocation);
        credentialsFilePath.ifPresent(path -> addProfileFile(builder, ProfileFile.Type.CREDENTIALS, path));

        return builder.build();
    }

    private static void addProfileFile(
            @NotNull final ProfileFile.Aggregator builder,
            @NotNull final ProfileFile.Type type,
            @NotNull final Path path) {
        builder.addFile(ProfileFile.builder()
                .type(type)
                .content(path.toAbsolutePath())
                .build());
    }
}
