/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.LogLevel;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LogBufferRecord implements Externalizable {

    private static final long serialVersionUID = 1L;

    private long timestampMicros;
    private LogLevel level;
    private ByteBuffer data;

    public long getTimestampMicros() {
        return timestampMicros;
    }

    public void setTimestampMicros(long timestampMicros) {
        this.timestampMicros = timestampMicros;
    }

    public LogLevel getLevel() {
        return level;
    }

    public void setLevel(LogLevel level) {
        this.level = level;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        Assert.assertion(data.hasArray(), "data.hasArray()");
        this.data = data;
    }

    public LogBufferRecord deepCopy() {
        final LogBufferRecord copy = new LogBufferRecord();
        copy.setTimestampMicros(timestampMicros);
        copy.setLevel(level);
        copy.setData(ByteBuffer.wrap(Arrays.copyOfRange(data.array(), data.position() + data.arrayOffset(),
                data.limit() + data.arrayOffset())));
        return copy;
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        timestampMicros = in.readLong();
        level = LogLevel.valueOf(in.readUTF());
        final byte[] dataArray = new byte[in.readInt()];
        in.readFully(dataArray);
        data = ByteBuffer.wrap(dataArray);
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        out.writeLong(timestampMicros);
        out.writeUTF(level.getName());
        out.writeInt(data.remaining());
        out.write(data.array(), data.position() + data.arrayOffset(), data.remaining());
    }

    public String getDataString() {
        getData().mark();
        try {
            return StandardCharsets.ISO_8859_1.newDecoder().decode(getData()).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalStateException("Failed to decode log data to a string", e);
        } finally {
            getData().reset();
        }
    }
}
