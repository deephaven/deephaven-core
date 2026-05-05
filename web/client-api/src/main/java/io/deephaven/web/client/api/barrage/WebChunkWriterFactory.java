//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.ByteChunkWriter;
import io.deephaven.extensions.barrage.chunk.CharChunkReader;
import io.deephaven.extensions.barrage.chunk.CharChunkWriter;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DoubleChunkWriter;
import io.deephaven.extensions.barrage.chunk.ExpansionKernel;
import io.deephaven.extensions.barrage.chunk.FloatChunkWriter;
import io.deephaven.extensions.barrage.chunk.IntChunkWriter;
import io.deephaven.extensions.barrage.chunk.ListChunkReader;
import io.deephaven.extensions.barrage.chunk.ListChunkWriter;
import io.deephaven.extensions.barrage.chunk.LongChunkWriter;
import io.deephaven.extensions.barrage.chunk.ShortChunkWriter;
import io.deephaven.extensions.barrage.chunk.VarBinaryChunkWriter;
import io.deephaven.extensions.barrage.chunk.array.ArrayExpansionKernel;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.base.Any;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FixedSizeList;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Type;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Creates writers for chunks. All chunks are expected to be ObjectChunks initially, this class will transform them as
 * needed to the appropriate types. Chunks should contain the correct data, but may have that masked as Any to allow
 * for null.
 */
public class WebChunkWriterFactory implements ChunkWriter.Factory {
    @Override
    public <T extends Chunk<Values>> ChunkWriter<T> newWriter(@NotNull BarrageTypeInfo<Field> typeInfo) {
        switch (typeInfo.arrowField().typeType()) {
            case Type.Int: {
                final Int t = new Int();
                typeInfo.arrowField().type(t);
                return (ChunkWriter<T>) switch (t.bitWidth()) {
                    case 8 -> new ByteChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                        WritableByteChunk<Values> output = WritableByteChunk.makeWritableChunk(objChunk.size());
                        for (int i = 0; i < objChunk.size(); i++) {
                            Any val = objChunk.get(i);
                            if (val != null) {
                                output.set(i, (byte) val.asInt());
                            }
                        }
                        return output;
                    }, empty(), true);
                    case 16 -> {
                        if (t.isSigned()) {
                            yield new ShortChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                                WritableShortChunk<Values> output = WritableShortChunk.makeWritableChunk(objChunk.size());
                                for (int i = 0; i < objChunk.size(); i++) {
                                    Any val = objChunk.get(i);
                                    if (val != null) {
                                        output.set(i, (short) val.asInt());
                                    }
                                }
                                return output;
                            }, empty(), true);
                        }
                        yield new CharChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                            WritableCharChunk<Values> output = WritableCharChunk.makeWritableChunk(objChunk.size());
                            for (int i = 0; i < objChunk.size(); i++) {
                                Any val = objChunk.get(i);
                                if (val != null) {
                                    output.set(i, (char) val.asInt());
                                }
                            }
                            return output;
                        }, empty(), true);
                    }
                    case 32 -> new IntChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                        WritableIntChunk<Values> output = WritableIntChunk.makeWritableChunk(objChunk.size());
                        for (int i = 0; i < objChunk.size(); i++) {
                            Any val = objChunk.get(i);
                            if (val != null) {
                                output.set(i, val.asInt());
                            }
                        }
                        return output;
                    }, empty(), true);
                    case 64 -> new LongChunkWriter<>((ObjectChunk<LongWrapper, Values> objChunk) -> {
                        final WritableLongChunk<Values> output = WritableLongChunk.makeWritableChunk(objChunk.size());
                        for (int ii = 0; ii < objChunk.size(); ++ii) {
                            LongWrapper val = objChunk.get(ii);
                            if (val != null) {
                                output.set(ii, val.getWrapped());
                            }
                        }
                        return output;
                    }, empty(), true);
                    default -> throw new UnsupportedOperationException(
                            "Unsupported int type width: " + t.bitWidth());
                };
            }
            case Type.FloatingPoint: {
                FloatingPoint t = new FloatingPoint();
                typeInfo.arrowField().type(t);
                return (ChunkWriter<T>) switch (t.precision()) {
                    case Precision.SINGLE -> new FloatChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                        WritableFloatChunk<Values> output = WritableFloatChunk.makeWritableChunk(objChunk.size());
                        for (int i = 0; i < objChunk.size(); i++) {
                            Any val = objChunk.get(i);
                            if (val != null) {
                                output.set(i, (float) val.asDouble());
                            }
                        }
                        return output;
                    }, empty(), true);
                    case Precision.DOUBLE -> new DoubleChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                        WritableDoubleChunk<Values> output = WritableDoubleChunk.makeWritableChunk(objChunk.size());
                        for (int i = 0; i < objChunk.size(); i++) {
                            Any val = objChunk.get(i);
                            if (val != null) {
                                output.set(i, val.asDouble());
                            }
                        }
                        return output;
                    }, empty(), true);
                    default -> throw new UnsupportedOperationException(
                            "Unsupported floating point precision: " + t.precision());
                };
            }
            case Type.Binary: {
            }
            case Type.Utf8: {
                return (ChunkWriter<T>) new VarBinaryChunkWriter<>(true,
                        (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
            }
            case Type.Bool: {
            }
            case Type.Date: {
            }
            case Type.Time: {
            }
            case Type.Timestamp: {
            }
            case Type.FixedSizeList:
            case Type.ListView:
            case Type.List: {
                final ListChunkReader.Mode listMode;
                int fixedSizeLength = 0;
                if (typeInfo.arrowField().typeType() == Type.FixedSizeList) {
                    listMode = ListChunkReader.Mode.FIXED;
                } else if (typeInfo.arrowField().typeType() == Type.ListView) {
                    listMode = ListChunkReader.Mode.VIEW;
                } else {
                    listMode = ListChunkReader.Mode.VARIABLE;
                    FixedSizeList list = new FixedSizeList();
                    typeInfo.arrowField().type(list);
                    fixedSizeLength = list.listSize();
                }
                Class<?> componentType = typeInfo.componentType();
                // TODO special case for byte[]?

                final BarrageTypeInfo<Field> componentTypeInfo = new BarrageTypeInfo<>(
                        componentType,
                        componentType == null ? null : componentType.getComponentType(),
                        typeInfo.arrowField().children(0));
                final ChunkType chunkType = ListChunkReader.getChunkTypeFor(componentTypeInfo.type());
                final ExpansionKernel<?> kernel =
                        ArrayExpansionKernel.makeExpansionKernel(chunkType, componentTypeInfo.type());
                final ChunkWriter<?> componentWriter = newWriter(componentTypeInfo);

                return (ChunkWriter<T>) new ListChunkWriter<>(listMode, fixedSizeLength, kernel, componentWriter, true);
            }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + Type.name(typeInfo.arrowField().typeType()));
        }
    }

    private static @NotNull <T> Supplier<ObjectChunk<T, Values>> empty() {
        return ObjectChunk::getEmptyChunk;
    }
}
