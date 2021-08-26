package io.deephaven.javascript.proto.dhinternal.grpcweb.chunkparser;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.ChunkParser.ChunkParser",
        namespace = JsPackage.GLOBAL)
public class ChunkParser {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ParseReturnType {
        @JsOverlay
        static ChunkParser.ParseReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        int getChunkType();

        @JsProperty
        Uint8Array getData();

        @JsProperty
        BrowserHeaders getTrailers();

        @JsProperty
        void setChunkType(int chunkType);

        @JsProperty
        void setData(Uint8Array data);

        @JsProperty
        void setTrailers(BrowserHeaders trailers);
    }

    public Uint8Array buffer;
    public double position;

    public native JsArray<ChunkParser.ParseReturnType> parse(Uint8Array bytes, boolean flush);

    public native JsArray<ChunkParser.ParseReturnType> parse(Uint8Array bytes);
}
