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
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.deephaven.configuration.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;

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
        final boolean compatibleIO = io instanceof S3FileIO;
        return compatibleScheme && compatibleIO;
    }

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final Class<?> ioClass) {
        final boolean compatibleScheme = S3Constants.S3_SCHEMES.contains(uriScheme);
        final boolean compatibleIO = S3FileIO.class.isAssignableFrom(ioClass);
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
                // We cannot check for `instanceOf S3CrtAsyncClient` because it is marked as @SdkInternalApi
                throw new IllegalArgumentException("S3 CRT client is not supported by Deephaven. Please set " +
                        "\"" + S3_CRT_ENABLED + "\" as \"false\" in catalog properties.");
                // TODO (DH-19253): Add support for S3CrtAsyncClient
            }

            final S3Instructions s3Instructions =
                    (specialInstructions == null) ? S3Instructions.DEFAULT : (S3Instructions) specialInstructions;
            final S3AsyncClient s3AsyncClient = ((S3FileIO) io).asyncClient();
            return UniversalS3SeekableChannelProviderPlugin.createUniversalS3Provider(
                    uriScheme, s3Instructions, s3AsyncClient);
        } else {
            return SeekableChannelsProviderLoader.getInstance().load(uriScheme, specialInstructions);
        }
    }
}
