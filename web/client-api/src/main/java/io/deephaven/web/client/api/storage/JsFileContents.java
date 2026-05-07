//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
 * Represents a file's contents loaded from the server. If an etag was specified when loading, client should first test
 * if the etag of this instance matches - if so, the contents will be empty, and the client's existing contents should
 * be used.
 */
@JsType(namespace = "dh.storage", name = "FileContents")
public class JsFileContents {

    /**
     * Creates an instance from an existing {@link Blob}.
     *
     * @param blob The blob containing the file contents.
     * @return A {@link JsFileContents} instance containing the provided data.
     */
    public static JsFileContents blob(Blob blob) {
        Objects.requireNonNull(blob, "Blob cannot be null");
        return new JsFileContents(blob, null);
    }

    /**
     * Creates an instance from text.
     *
     * @param text One or more string parts to combine into a {@link Blob}.
     * @return A {@link JsFileContents} instance containing the provided data.
     */
    public static JsFileContents text(String... text) {
        return new JsFileContents(new Blob(JsArray.from(text)), null);
    }

    /**
     * Creates an instance from one or more {@link ArrayBuffer} values.
     *
     * @param buffers One or more buffers to combine into a {@link Blob}.
     * @return A {@link JsFileContents} instance containing the provided data.
     */
    public static JsFileContents arrayBuffers(ArrayBuffer... buffers) {
        return new JsFileContents(new Blob(JsArray.from(buffers)), null);
    }

    private final Blob data;

    private final String etag;

    @JsIgnore
    public JsFileContents(Blob data, String etag) {
        this.data = data;
        this.etag = etag;
    }

    @JsIgnore
    public JsFileContents(String etag) {
        this.data = null;
        this.etag = etag;
    }

    private Promise<Blob> contents() {
        if (data != null) {
            return Promise.resolve(data);
        }
        return Promise.reject("No contents available, please use provided etag");
    }

    /**
     * Reads the contents as text.
     *
     * @return A promise that resolves to the file contents as a string.
     */
    @JsMethod
    public Promise<String> text() {
        return contents().then(Blob::text);
    }

    /**
     * Reads the contents as an {@link ArrayBuffer}.
     *
     * @return A promise that resolves to the file contents as an {@link ArrayBuffer}.
     */
    @JsMethod
    public Promise<ArrayBuffer> arrayBuffer() {
        return contents().then(Blob::arrayBuffer);
    }

    /**
     * The etag associated with the contents.
     *
     * <p>
     * If an etag was provided when loading and the server indicates that the client's cached contents are still valid,
     * the contents of this instance may be empty and only the etag will be returned.
     */
    @JsProperty
    public String getEtag() {
        return etag;
    }
}
