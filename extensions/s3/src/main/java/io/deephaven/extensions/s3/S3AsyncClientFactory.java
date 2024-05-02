//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;

public interface S3AsyncClientFactory {
    /**
     * @return a new S3AsyncClient
     */
    S3AsyncClient create();
}
