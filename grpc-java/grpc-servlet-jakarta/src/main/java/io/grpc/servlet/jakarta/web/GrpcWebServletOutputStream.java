package io.grpc.servlet.jakarta.web;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;

public class GrpcWebServletOutputStream extends ServletOutputStream {
    private final ServletOutputStream wrapped;
    private boolean readyForFrame = true;

    public GrpcWebServletOutputStream(ServletOutputStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean isReady() {
        return wrapped.isReady();
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {
        wrapped.setWriteListener(writeListener);
    }

    @Override
    public void write(int i) throws IOException {
        // TODO handle buffered impl too

        // intercept write and insert message framing
        // if (readyForFrame) {
        // wrapped.write();
        // }
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
        super.flush();
        // ready to start a new frame
        readyForFrame = true;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
