//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util.internal;

import com.google.auto.service.AutoService;
import io.deephaven.extensions.s3.CredentialsPropertyAdapterInternal;
import io.deephaven.extensions.s3.S3Instructions;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.net.URI;
import java.util.Map;

@AutoService(io.deephaven.iceberg.util.internal.PropertyAdapter.class)
public final class S3InstructionsPropertyAdapter implements PropertyAdapter {

    public static void adapt(S3Instructions instructions, Map<String, String> propertiesOut) {
        instructions.regionName().ifPresent(region -> propertiesOut.put(AwsClientProperties.CLIENT_REGION, region));
        CredentialsPropertyAdapterInternal.adapt(instructions.credentials(), propertiesOut);
        instructions.endpointOverride().map(URI::toString)
                .ifPresent(uri -> propertiesOut.put(S3FileIOProperties.ENDPOINT, uri));
    }

    public S3InstructionsPropertyAdapter() {}

    @Override
    public void adapt(Object dataInstructions, Map<String, String> propertiesOut) {
        if (dataInstructions instanceof S3Instructions) {
            adapt((S3Instructions) dataInstructions, propertiesOut);
        }
    }
}
