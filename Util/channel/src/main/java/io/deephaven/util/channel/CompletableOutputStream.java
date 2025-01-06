//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An {@link OutputStream} that can be marked as done, completed, or rolled back.
 * <p>
 * The {@link #done()} method is used to flush all buffered data to the underlying storage, {@link #complete()} to
 * finalize the write operation, and {@link #rollback()} to cancel the write. Closing this output stream without calling
 * complete will not flush data to the underlying storage.
 * <p>
 * One usage pattern can be like this:
 * 
 * <pre>
 * try (final CompletableOutputStream outputStream = CreateCompletableOutputStream()) {
 *     try {
 *         IOUtils.copy(inputStream, outputStream);
 *         outputStream.done(); // Optional; use this to flush buffered data without completing the stream
 *         outputStream.complete();
 *     } catch (IOException e) {
 *         outputStream.rollback();
 *     }
 * }
 * </pre>
 */
public abstract class CompletableOutputStream extends OutputStream {

    /**
     * Flush all buffered data to the underlying storage. This is optional and should be called after the user is done
     * writing to the output stream. All writes to the output stream after calling this method will lead to an
     * {@link IOException}.
     */
    public abstract void done() throws IOException;

    /**
     * Flush all buffered data and save all written data to the underlying storage. This method should be called after
     * the user is done writing to the output stream. All writes to the output stream after calling this method will
     * lead to an {@link IOException}.
     */
    public abstract void complete() throws IOException;

    /**
     * Try to roll back any data written to the underlying storage, reverting back to the original state before opening
     * this stream. This is an optional operation, as some implementations may not be able to support it.
     */
    public abstract void rollback() throws IOException;
}
