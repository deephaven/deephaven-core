//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.storage;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.Storage_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.ItemInfo;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Storage service metadata about files and folders.
 */
@JsType(namespace = "dh.storage", name = "ItemDetails")
public class JsItemDetails {
    private final String parentPath;
    private final String path;
    private final int type;
    private final String size;
    private final String etag;

    @JsIgnore
    public JsItemDetails(String parentPath, String path, int kind, String size, String etag) {
        this.parentPath = parentPath;
        this.path = path;
        this.type = kind;
        this.size = size;
        this.etag = etag;
    }

    @JsIgnore
    public static JsItemDetails fromProto(String parentPath, ItemInfo item) {
        return new JsItemDetails(parentPath, item.getPath(), item.getType(), item.getSize(), item.getEtag());
    }

    @JsProperty
    public String getFilename() {
        return path;
    }

    @JsProperty
    public String getBasename() {
        // TODO (deephaven-core#5068) remove extra check
        if (parentPath.equals("/")) {
            return path.substring(1);
        }
        return path.substring(parentPath.length() + 1);
    }

    @JsProperty
    public String getDirname() {
        return parentPath;
    }

    @JsProperty
    @TsTypeRef(JsItemType.class)
    public String getType() {
        return type == Storage_pb.ItemType.getDIRECTORY() ? JsItemType.DIRECTORY : JsItemType.FILE;
    }

    @JsProperty
    public double getSize() {
        return size == null ? 0 : Double.parseDouble(size);
    }

    @JsProperty
    public String getEtag() {
        return etag;
    }

    @JsMethod
    @Override
    public String toString() {
        return getBasename();
    }
}
