package io.deephaven.web.client.api.storage;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Notebook_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.FileInfo;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class ItemDetails {
    private final String path;
    private final int kind;
    private final String size;

    @JsMethod(namespace = "dh.storage.ItemDetails")
    public static ItemDetails file(String... pathParts) {
        return new ItemDetails(JsArray.asJsArray(pathParts).join("/"), Notebook_pb.FileKind.getFILE(), null);
    }

    @JsMethod(namespace = "dh.storage.ItemDetails")
    public static ItemDetails directory(String... pathParts) {
        return new ItemDetails(JsArray.asJsArray(pathParts).join("/"), Notebook_pb.FileKind.getDIRECTORY(), null);
    }

    public ItemDetails(String path, int kind, String size) {
        this.path = path;
        this.kind = kind;
        this.size = size;
    }

    public static ItemDetails fromProto(FileInfo item) {
        return new ItemDetails(item.getPath(), item.getKind(), item.getSize());
    }

    @JsProperty
    public String getFilename() {
        return path.substring(path.lastIndexOf('/'));
    }
    @JsProperty
    public String getBasename() {
        return path.substring(0, path.lastIndexOf('/'));
    }
    @JsProperty
    public String getPath() {
        return path;
    }
    @JsProperty
    public String getKind() {
        return kind == Notebook_pb.FileKind.getDIRECTORY() ? "directory" : "file";
    }
    @JsProperty
    public Double getSize() {
        return size == null ? null : Double.parseDouble(size);
    }
}
