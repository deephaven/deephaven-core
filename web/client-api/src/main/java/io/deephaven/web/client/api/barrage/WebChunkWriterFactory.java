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
import io.deephaven.extensions.barrage.chunk.BooleanChunkWriter;
import io.deephaven.extensions.barrage.chunk.ByteChunkWriter;
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
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LocalDateWrapper;
import io.deephaven.web.client.api.LocalTimeWrapper;
import io.deephaven.web.client.api.LongWrapper;
import elemental2.core.JsDate;
import jsinterop.base.Any;
import org.apache.arrow.flatbuf.Date;
import org.apache.arrow.flatbuf.DateUnit;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FixedSizeList;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Time;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.Timestamp;
import org.apache.arrow.flatbuf.Type;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Creates writers for chunks. All chunks are expected to be ObjectChunks initially, this class will transform them as
 * needed to the appropriate types. Chunks should contain the correct data, but may have that masked as Any to allow for
 * null.
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
                            } else {
                                output.set(i, QueryConstants.NULL_BYTE);
                            }
                        }
                        return output;
                    }, empty(), true);
                    case 16 -> {
                        if (t.isSigned()) {
                            yield new ShortChunkWriter<>((ObjectChunk<Any, Values> objChunk) -> {
                                WritableShortChunk<Values> output =
                                        WritableShortChunk.makeWritableChunk(objChunk.size());
                                for (int i = 0; i < objChunk.size(); i++) {
                                    Any val = objChunk.get(i);
                                    if (val != null) {
                                        output.set(i, (short) val.asInt());
                                    } else {
                                        output.set(i, QueryConstants.NULL_SHORT);
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
                                } else {
                                    output.set(i, QueryConstants.NULL_CHAR);
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
                            } else {
                                output.set(i, QueryConstants.NULL_INT);
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
                            } else {
                                output.set(ii, QueryConstants.NULL_LONG);
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
                            } else {
                                output.set(i, QueryConstants.NULL_FLOAT);
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
                            } else {
                                output.set(i, QueryConstants.NULL_DOUBLE);
                            }
                        }
                        return output;
                    }, empty(), true);
                    default -> throw new UnsupportedOperationException(
                            "Unsupported floating point precision: " + t.precision());
                };
            }
            case Type.Binary: {
                if (typeInfo.type() == BigIntegerWrapper.class) {
                    return (ChunkWriter<T>) new VarBinaryChunkWriter<BigIntegerWrapper>(true,
                            (out, item) -> out.write(item.getWrapped().toByteArray()));
                }
                if (typeInfo.type() == BigDecimalWrapper.class) {
                    return (ChunkWriter<T>) new VarBinaryChunkWriter<BigDecimalWrapper>(true,
                            (out, item) -> {
                                final int scale = item.getWrapped().scale();
                                // write scale as little-endian int
                                out.write(scale & 0xFF);
                                out.write((scale >> 8) & 0xFF);
                                out.write((scale >> 16) & 0xFF);
                                out.write((scale >> 24) & 0xFF);
                                out.write(item.getWrapped().unscaledValue().toByteArray());
                            });
                }
                throw new UnsupportedOperationException("Unsupported Binary type " + typeInfo.type());
            }
            case Type.Utf8: {
                return (ChunkWriter<T>) new VarBinaryChunkWriter<>(true,
                        (out, item) -> out.write(item.toString().getBytes(StandardCharsets.UTF_8)));
            }
            case Type.Bool: {
                return (ChunkWriter<T>) new BooleanChunkWriter<>((ObjectChunk<Boolean, Values> objChunk) -> {
                    WritableByteChunk<Values> output = WritableByteChunk.makeWritableChunk(objChunk.size());
                    for (int i = 0; i < objChunk.size(); i++) {
                        Boolean val = objChunk.get(i);
                        output.set(i, BooleanUtils.booleanAsByte(val));
                    }
                    return output;
                }, empty(), true);
            }
            case Type.Date: {
                Date d = new Date();
                typeInfo.arrowField().type(d);
                switch (d.unit()) {
                    case DateUnit.MILLISECOND:
                        return (ChunkWriter<T>) new LongChunkWriter<>(
                                (ObjectChunk<LocalDateWrapper, Values> objChunk) -> {
                                    final WritableLongChunk<Values> output =
                                            WritableLongChunk.makeWritableChunk(objChunk.size());
                                    for (int i = 0; i < objChunk.size(); i++) {
                                        LocalDateWrapper val = objChunk.get(i);
                                        if (val != null) {
                                            JsDate jsDate = new JsDate();
                                            jsDate.setUTCFullYear(val.getYear(), val.getMonthValue() - 1,
                                                    val.getDayOfMonth());
                                            jsDate.setUTCHours(0, 0, 0, 0);
                                            output.set(i, (long) jsDate.getTime());
                                        } else {
                                            output.set(i, QueryConstants.NULL_LONG);
                                        }
                                    }
                                    return output;
                                }, empty(), true);
                    case DateUnit.DAY:
                        return (ChunkWriter<T>) new IntChunkWriter<>(
                                (ObjectChunk<LocalDateWrapper, Values> objChunk) -> {
                                    WritableIntChunk<Values> output =
                                            WritableIntChunk.makeWritableChunk(objChunk.size());
                                    for (int i = 0; i < objChunk.size(); i++) {
                                        LocalDateWrapper val = objChunk.get(i);
                                        if (val != null) {
                                            JsDate jsDate = new JsDate();
                                            jsDate.setUTCFullYear(val.getYear(), val.getMonthValue() - 1,
                                                    val.getDayOfMonth());
                                            jsDate.setUTCHours(0, 0, 0, 0);
                                            output.set(i, (int) (jsDate.getTime() / 86400000));
                                        } else {
                                            output.set(i, QueryConstants.NULL_INT);
                                        }
                                    }
                                    return output;
                                }, empty(), true);
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported Date unit: " + DateUnit.name(d.unit()));
                }
            }
            case Type.Time: {
                Time t = new Time();
                typeInfo.arrowField().type(t);
                switch (t.bitWidth()) {
                    case 32: {
                        final int unitsPerSecond;
                        switch (t.unit()) {
                            case TimeUnit.SECOND:
                                unitsPerSecond = 1;
                                break;
                            case TimeUnit.MILLISECOND:
                                unitsPerSecond = 1_000;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported Time unit: " + TimeUnit.name(t.unit()));
                        }
                        return (ChunkWriter<T>) new IntChunkWriter<>(
                                (ObjectChunk<LocalTimeWrapper, Values> objChunk) -> {
                                    WritableIntChunk<Values> output =
                                            WritableIntChunk.makeWritableChunk(objChunk.size());
                                    for (int i = 0; i < objChunk.size(); i++) {
                                        LocalTimeWrapper val = objChunk.get(i);
                                        if (val != null) {
                                            output.set(i, val.toInt(unitsPerSecond));
                                        } else {
                                            output.set(i, QueryConstants.NULL_INT);
                                        }
                                    }
                                    return output;
                                }, empty(), true);
                    }
                    case 64: {
                        final int unitsPerSecond;
                        switch (t.unit()) {
                            case TimeUnit.NANOSECOND:
                                unitsPerSecond = 1_000_000_000;
                                break;
                            case TimeUnit.MICROSECOND:
                                unitsPerSecond = 1_000_000;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported Time unit: " + TimeUnit.name(t.unit()));
                        }
                        return (ChunkWriter<T>) new LongChunkWriter<>(
                                (ObjectChunk<LocalTimeWrapper, Values> objChunk) -> {
                                    final WritableLongChunk<Values> output =
                                            WritableLongChunk.makeWritableChunk(objChunk.size());
                                    for (int i = 0; i < objChunk.size(); i++) {
                                        LocalTimeWrapper val = objChunk.get(i);
                                        if (val != null) {
                                            output.set(i, val.toLong(unitsPerSecond));
                                        } else {
                                            output.set(i, QueryConstants.NULL_LONG);
                                        }
                                    }
                                    return output;
                                }, empty(), true);
                    }
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported Time bitWidth: " + t.bitWidth());
                }
            }
            case Type.Timestamp: {
                Timestamp ts = new Timestamp();
                typeInfo.arrowField().type(ts);
                switch (ts.unit()) {
                    case TimeUnit.NANOSECOND: {
                        if (!ts.timezone().equals("UTC")) {
                            throw new UnsupportedOperationException("Unsupported tz " + ts.timezone());
                        }
                        return (ChunkWriter<T>) new LongChunkWriter<>((ObjectChunk<DateWrapper, Values> objChunk) -> {
                            final WritableLongChunk<Values> output =
                                    WritableLongChunk.makeWritableChunk(objChunk.size());
                            for (int i = 0; i < objChunk.size(); i++) {
                                DateWrapper val = objChunk.get(i);
                                if (val != null) {
                                    output.set(i, val.getWrapped());
                                } else {
                                    output.set(i, QueryConstants.NULL_LONG);
                                }
                            }
                            return output;
                        }, empty(), true);
                    }
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported Timestamp unit: " + TimeUnit.name(ts.unit()));
                }
            }
            case Type.FixedSizeList:
            case Type.ListView:
            case Type.List: {
                final ListChunkReader.Mode listMode;
                int fixedSizeLength = 0;
                if (typeInfo.arrowField().typeType() == Type.FixedSizeList) {
                    listMode = ListChunkReader.Mode.FIXED;
                    FixedSizeList list = new FixedSizeList();
                    typeInfo.arrowField().type(list);
                    fixedSizeLength = list.listSize();
                } else if (typeInfo.arrowField().typeType() == Type.ListView) {
                    listMode = ListChunkReader.Mode.VIEW;
                } else {
                    listMode = ListChunkReader.Mode.VARIABLE;
                }
                Class<?> componentType = typeInfo.componentType();
                // TODO special case for byte[]?

                assert componentType != null;
                final BarrageTypeInfo<Field> componentTypeInfo = BarrageTypeInfo.make(
                        componentType,
                        componentType.getComponentType(),
                        typeInfo.arrowField().children(0));
                final ChunkType chunkType = ListChunkReader.getChunkTypeFor(componentTypeInfo.type());
                final ExpansionKernel<?> kernel =
                        ArrayExpansionKernel.makeExpansionKernel(chunkType, componentTypeInfo.type());
                final ChunkWriter<?> componentWriter = newWriter(componentTypeInfo);

                // noinspection unchecked
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
