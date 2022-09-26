package io.deephaven.web.client.api.storage;

import elemental2.core.ArrayBuffer;
import elemental2.core.JsArray;
import elemental2.dom.Blob;
import elemental2.promise.Promise;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

import java.util.Objects;

/**
 * Represents a file's contents loaded from the server. If an etag was specified when loading, client should first
 * test if the etag of this instance matches - if so, the contents will be empty, and the client's existing contents
 * should be used.
 */
@JsType(namespace = "dh.storage")
public class FileContents {

    public static FileContents blob(Blob blob) {
        Objects.requireNonNull(blob, "Blob cannot be null");
        return new FileContents(blob, null);
    }

    public static FileContents text(String... text) {
        return new FileContents(new Blob(JsArray.from(text)), null);
    }

    public static FileContents arrayBuffers(ArrayBuffer... buffers) {
        return new FileContents(new Blob(JsArray.from(buffers)), null);
    }

    private final Blob data;

    private final String etag;

    @JsIgnore
    public FileContents(Blob data, String etag) {
        this.data = data;
        this.etag = etag;
    }

    @JsIgnore
    public FileContents(String etag) {
        this.data = null;
        this.etag = etag;
    }

    private Promise<Blob> contents() {
        if (data != null) {
            return Promise.resolve(data);
        }
        return Promise.reject("No contents available, please use provided etag");
    }

    @JsMethod
    public Promise<String> text() {
        return contents().then(Blob::text);
    }

    @JsMethod
    public Promise<ArrayBuffer> arrayBuffer() {
        return contents().then(Blob::arrayBuffer);
    }

    @JsProperty
    public String getEtag() {
        return etag;
    }
}
