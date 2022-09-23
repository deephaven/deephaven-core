package io.deephaven.web.client.api.storage;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Storage_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.storage_pb.FileInfo;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class JsItemDetails {
    private final String path;
    private final int type;
    private final String size;

    @JsMethod(namespace = "dh.storage.ItemDetails")
    public static JsItemDetails file(String... pathParts) {
        return new JsItemDetails(JsArray.asJsArray(pathParts).join("/"), Storage_pb.FileKind.getFILE(), null);
    }

    @JsMethod(namespace = "dh.storage.ItemDetails")
    public static JsItemDetails directory(String... pathParts) {
        return new JsItemDetails(JsArray.asJsArray(pathParts).join("/"), Storage_pb.FileKind.getDIRECTORY(), null);
    }

    public JsItemDetails(String path, int kind, String size) {
        this.path = path;
        this.type = kind;
        this.size = size;
    }

    public static JsItemDetails fromProto(FileInfo item) {
        return new JsItemDetails(item.getPath(), item.getKind(), item.getSize());
    }

    @JsProperty
    public String getFilename() {
        return path;
    }

    @JsProperty
    public String getBasename() {
        return path.substring(path.lastIndexOf('/'));
    }

    @JsProperty
    public String getDirname() {
        return path.substring(0, path.lastIndexOf('/'));
    }

    @JsProperty
    public String getType() {
        return type == Storage_pb.FileKind.getDIRECTORY() ? "directory" : "file";
    }

    @JsProperty
    public double getSize() {
        return size == null ? 0 : Double.parseDouble(size);
    }

    @JsProperty
    public String getEtag() {
        return null;
    }
}
