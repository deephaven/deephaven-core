//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link OutputStream} that can be marked as done, completed, or rolled back.
 * <p>
 * The {@link #done()} method is to push all cached data to the underlying storage, {@link #complete()} to finalize the
 * write operation, and {@link #rollback()} to cancel the write. Closing this output stream without calling done or
 * complete will not write any data to the underlying storage.
 * <p>
 * One usage pattern can be like this:
 * 
 * <pre>
 * try (final CompletableOutputStream outputStream = CreateCompletableOutputStream()) {
 *     try {
 *         IOUtils.copy(inputStream, outputStream);
 *         outputStream.done();
 *         outputStream.close();
 *     } catch (IOException e) {
 *         outputStream.rollback();
 *     }
 * }
 * </pre>
 */
public abstract class CompletableOutputStream extends OutputStream {
    /**
     * Pushes all cached data to the underlying storage. This method should be called after the user is done writing to
     * the output stream. All writes to the output stream after calling this method will lead to an {@link IOException}.
     */
    public abstract void done() throws IOException;

    /**
     * Push all cached data to underlying storage and commit the data to the underlying storage. This method should be
     * called after the user is done writing to the output stream. All writes to the output stream after calling this
     * method will lead to an {@link IOException}.
     */
    public abstract void complete() throws IOException;

    /**
     * Try to roll back any data committed to the underlying storage, reverting back to the original state before
     * opening this stream.
     */
    public abstract void rollback() throws IOException;
}
