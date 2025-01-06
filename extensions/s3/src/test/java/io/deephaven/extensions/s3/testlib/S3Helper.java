//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3.testlib;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class S3Helper {

    /**
     * Timeout in seconds for S3 operations for testing.
     */
    public static final int TIMEOUT_SECONDS = 30;

    public static void uploadDirectory(
            S3AsyncClient s3AsyncClient,
            Path dir,
            String bucket,
            String prefix,
            Duration timeout) throws ExecutionException, InterruptedException, TimeoutException {
        try (final S3TransferManager manager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            uploadDirectory(manager, dir, bucket, prefix, timeout);
        }
    }

    private static void uploadDirectory(
            S3TransferManager transferManager,
            Path dir,
            String bucket,
            String prefix,
            Duration timeout) throws ExecutionException, InterruptedException, TimeoutException {
        // Not a way to get a list of the uploaded files, even when using a TransferListener.
        final DirectoryUpload directoryUpload = transferManager.uploadDirectory(UploadDirectoryRequest.builder()
                .source(dir)
                .bucket(bucket)
                .s3Prefix(prefix)
                .build());
        final CompletedDirectoryUpload upload =
                directoryUpload.completionFuture().get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        if (!upload.failedTransfers().isEmpty()) {
            throw new RuntimeException("Upload has failed transfers");
        }
    }

    public static void deleteAllKeys(S3AsyncClient s3AsyncClient, String bucket)
            throws ExecutionException, InterruptedException, TimeoutException {
        ListObjectsV2Response response = s3AsyncClient
                .listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build())
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final List<CompletableFuture<?>> futures = new ArrayList<>();
        while (true) {
            final List<ObjectIdentifier> deletes = response.contents()
                    .stream()
                    .map(S3Object::key)
                    .map(S3Helper::objectId)
                    .collect(Collectors.toList());
            if (deletes.isEmpty()) {
                break;
            }
            futures.add(s3AsyncClient.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder().objects(deletes).build())
                    .build()));
            final String nextContinuationToken = response.nextContinuationToken();
            if (nextContinuationToken == null) {
                break;
            }
            response = s3AsyncClient.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(bucket).continuationToken(nextContinuationToken).build())
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        if (futures.isEmpty()) {
            return;
        }
        CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static ObjectIdentifier objectId(String o) {
        return ObjectIdentifier.builder().key(o).build();
    }
}
