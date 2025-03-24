//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.BaseSeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.datastructures.ThreadSafeMaxSizePool;
import org.jetbrains.annotations.NotNull;

/**
 * Context object used to store buffer pool for write requests.
 */
final class S3WriteContext extends BaseSeekableChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3WriteContext.class);

    /**
     * Pool of {@link S3WriteRequest} objects used to write to S3. This pool is thread-safe and has a fixed size, which
     * helps to limit the maximum number of concurrent write requests.
     */
    final Pool<S3WriteRequest> requestPool;

    S3WriteContext(@NotNull final S3Instructions instructions) {
        this.requestPool = new ThreadSafeMaxSizePool<>(
                instructions.numConcurrentWriteParts(),
                () -> new S3WriteRequest(instructions.writePartSize()),
                S3WriteRequest::reset);

        if (log.isDebugEnabled()) {
            log.debug().append("Creating output stream context").endl();
        }
    }
}
