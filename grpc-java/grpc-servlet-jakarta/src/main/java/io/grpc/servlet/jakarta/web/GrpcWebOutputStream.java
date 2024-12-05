/*
 * Copyright 2019 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.grpc.servlet.jakarta.web;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps the usual ServletOutputStream so as to allow downstream writers to use it according to the servlet spec, but
 * still make it easy to write trailers as a payload instead of using HTTP trailers at the end of a stream.
 */
public class GrpcWebOutputStream extends ServletOutputStream implements WriteListener {
    private final ServletOutputStream wrapped;
    private final GrpcWebServletResponse grpcWebServletResponse;

    // Access to these are guarded by synchronized
    private Runnable waiting;
    private WriteListener writeListener;

    public GrpcWebOutputStream(ServletOutputStream wrapped, GrpcWebServletResponse grpcWebServletResponse) {
        this.wrapped = wrapped;
        this.grpcWebServletResponse = grpcWebServletResponse;
    }

    @Override
    public boolean isReady() {
        return wrapped.isReady();
    }

    /**
     * Internal helper method to correctly write the given bytes, then complete the stream.
     *
     * @param bytes the bytes to write once writing is possible
     * @param close a Runnable to invoke once this write is complete
     */
    public synchronized void writeAndCloseWhenReady(byte[] bytes, Runnable close) {
        if (writeListener == null) {
            throw new IllegalStateException("writeListener");
        }
        if (isReady()) {
            try {
                write(bytes);
            } catch (IOException ignored) {
                // Ignore this error, we're closing anyway
            } finally {
                close.run();
            }
        } else {
            waiting = () -> {
                try {
                    write(bytes);
                } catch (IOException e) {
                    // ignore this, we're closing anyway
                } finally {
                    close.run();
                }
            };
        }
    }

    @Override
    public synchronized void setWriteListener(WriteListener writeListener) {
        this.writeListener = writeListener;
        wrapped.setWriteListener(this);
    }

    @Override
    public void write(int i) throws IOException {
        wrapped.write(i);
    }

    @Override
    public void write(byte[] b) throws IOException {
        wrapped.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        wrapped.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        wrapped.flush();
    }

    @Override
    public void close() throws IOException {
        // Since we're a grpc-web response, we must write trailers on our way out as part of close - but trailers
        // for grpc-web are a data frame, not HTTP trailers. Call up to the response to write the trailer frame,
        // then close the underlying stream.
        AtomicReference<IOException> exception = new AtomicReference<>();
        grpcWebServletResponse.writeTrailers(() -> {
            try {
                wrapped.close();
            } catch (IOException e) {
                exception.set(e);
            }
        });
        IOException ex = exception.get();
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public synchronized void onWritePossible() throws IOException {
        if (writeListener != null) {
            writeListener.onWritePossible();
        }
        if (waiting != null) {
            waiting.run();
            waiting = null;
        }
    }

    @Override
    public void onError(Throwable t) {
        if (writeListener != null) {
            writeListener.onError(t);
        }
    }
}
