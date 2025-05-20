//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.iceberg.util.FileIOAdapterBase;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.deephaven.configuration.Configuration;

import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.S3_CRT_ENABLED_DEFAULT;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files to S3.
 */
@AutoService(FileIOAdapter.class)
public final class S3FileIOAdapter extends FileIOAdapterBase {

    private static final boolean USE_S3_CLIENT_FROM_FILE_IO = Configuration.getInstance().getBooleanWithDefault(
            "Iceberg.useS3ClientFromFileIO", true);

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final FileIO io) {
        final boolean compatibleScheme = S3Constants.S3_SCHEMES.contains(uriScheme);
        final boolean compatibleIO = io instanceof S3FileIO || io instanceof ResolvingFileIO;
        return compatibleScheme && compatibleIO;
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions) {
        if (!isCompatible(uriScheme, io)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme +
                    ", io " + io.getClass().getName() + ", special instructions " + specialInstructions);
        }
        if (USE_S3_CLIENT_FROM_FILE_IO) {
            if (PropertyUtil.propertyAsBoolean(io.properties(), S3_CRT_ENABLED, S3_CRT_ENABLED_DEFAULT)) {
                throw new IllegalArgumentException(
                        "S3 CRT is not supported by Deephaven. Please set \"" + S3_CRT_ENABLED +
                                "\" as \"false\" in catalog properties.");
                // TODO (DH-19253): Experiment with and add support for S3CrtAsyncClient
                // TODO Check with Devin what would be a good place to put this check.
            }
            if (specialInstructions != null && !(specialInstructions instanceof S3Instructions)) {
                throw new IllegalArgumentException("Special instructions must be of type S3Instructions");
            }
            final S3FileIO s3FileIO = (S3FileIO) io;
            final S3Instructions s3Instructions =
                    specialInstructions == null ? S3Instructions.DEFAULT : (S3Instructions) specialInstructions;

            // Use the S3 clients from the S3FileIO
            final S3Instructions useInstructions =
                    s3Instructions.withS3AsyncClient(s3FileIO.asyncClient()).withS3Client(s3FileIO.client());
            return SeekableChannelsProviderLoader.getInstance().load(uriScheme, useInstructions);
        } else {
            return SeekableChannelsProviderLoader.getInstance().load(uriScheme, specialInstructions);
        }
    }
}
