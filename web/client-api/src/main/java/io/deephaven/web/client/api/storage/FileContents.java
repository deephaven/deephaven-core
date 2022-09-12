package io.deephaven.web.client.api.storage;

import elemental2.core.ArrayBuffer;
import elemental2.core.JsArray;
import elemental2.dom.Blob;
import elemental2.promise.Promise;
import jsinterop.annotations.JsMethod;

public class FileContents {

    @JsMethod(namespace = "dh.storage.FileContents")
    public static FileContents blob(Blob blob) {
        return new FileContents(blob);
    }
    @JsMethod(namespace = "dh.storage.FileContents")
    public static FileContents text(String... text) {
        return new FileContents(new Blob(JsArray.from(text)));
    }
    @JsMethod(namespace = "dh.storage.FileContents")
    public static FileContents arrayBuffers(ArrayBuffer... buffers) {
        return new FileContents(new Blob(JsArray.from(buffers)));
    }

    private final Blob data;

    public FileContents(Blob data) {
        this.data = data;
    }

    @JsMethod
    public Promise<String> text() {
        return data.text();
    }

    @JsMethod
    public Promise<ArrayBuffer> arrayBuffer() {
        return data.arrayBuffer();
    }
}
