package io.grpc.servlet.jakarta.web;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;

import java.io.IOException;

/**
 * Wraps the usual ServletOutputStream so as to allow downstream writers to use it according to the servlet spec, but
 * still make it easy to write trailers as a payload instead of using HTTP trailers at the end of a stream.
 */
public class GrpcWebOutputStream extends ServletOutputStream implements WriteListener {
    private final ServletOutputStream wrapped;

    // Access to these are guarded by synchronized
    private Runnable waiting;
    private WriteListener writeListener;

    public GrpcWebOutputStream(ServletOutputStream wrapped) {
        this.wrapped = wrapped;
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
    public synchronized void writeAndCloseWhenReady(byte[] bytes, Runnable close) throws IOException {
        if (writeListener == null) {
            throw new IllegalStateException("writeListener");
        }
        if (isReady()) {
            try {
                write(bytes);
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
        wrapped.close();
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
