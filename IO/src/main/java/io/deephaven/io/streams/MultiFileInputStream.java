/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.io.IOException;

public class MultiFileInputStream extends InputStream {

    // Just used for logging
    public static class DecoratedInputStream {
        private final String filename;
        private final InputStream inputStream;

        public DecoratedInputStream(String filename, InputStream inputStream) {
            this.filename = filename;
            this.inputStream = inputStream;
        }
    }

    private final DecoratedInputStream inputStreams[];
    private int currentStream = -1;

    /*
     * Note that MultiFileInputStream assumes ownership for all streams and will close them when it is closed.
     */
    public MultiFileInputStream(DecoratedInputStream inputStreams[]) {
        this.inputStreams = inputStreams;
    }

    @Override
    public int read() throws IOException {
        if (inputStreams.length == 0 || (currentStream >= inputStreams.length)) {
            return -1;
        } else if (currentStream < 0) {
            currentStream++;
        }

        int result = inputStreams[currentStream].inputStream.read();
        while (result == -1) {
            currentStream++;
            if (currentStream < inputStreams.length) {
                result = read();
            } else {
                return -1;
            }
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (inputStreams.length == 0 || (currentStream >= inputStreams.length)) {
            return -1;
        } else if (currentStream < 0) {
            currentStream++;
        }

        int result = 0;
        do {
            int readBytes = inputStreams[currentStream].inputStream.read(b, off, len);
            if (readBytes >= 0) {
                len -= readBytes;
                off += readBytes;
                result += readBytes;
            }
            if (len > 0) {
                currentStream++;
                if (currentStream >= inputStreams.length) {
                    if (result == 0) {
                        return -1;
                    }
                    return result;
                }
            }
        } while (len > 0);
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        IOException caughtException = null;
        for (DecoratedInputStream is : inputStreams) {
            try {
                is.inputStream.close();
            } catch (IOException e) {
                caughtException = e;
            }
        }
        if (caughtException != null) {
            throw caughtException;
        }
    }

    /**
     * Get the filename of the current stream
     *
     * @return filename, null if no current file
     */
    public @Nullable String getCurrentFilename() {
        if (currentStream < inputStreams.length) {
            return inputStreams[currentStream].filename;
        }
        return null;
    }
}
