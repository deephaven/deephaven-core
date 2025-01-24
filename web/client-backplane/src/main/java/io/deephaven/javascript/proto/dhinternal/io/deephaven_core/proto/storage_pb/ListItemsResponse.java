//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb.ListItemsResponse",
        namespace = JsPackage.GLOBAL)
public class ListItemsResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsOverlay
            static ListItemsResponse.ToObjectReturnType.ItemsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getEtag();

            @JsProperty
            String getPath();

            @JsProperty
            String getSize();

            @JsProperty
            double getType();

            @JsProperty
            void setEtag(String etag);

            @JsProperty
            void setPath(String path);

            @JsProperty
            void setSize(String size);

            @JsProperty
            void setType(double type);
        }

        @JsOverlay
        static ListItemsResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getCanonicalPath();

        @JsProperty
        JsArray<ListItemsResponse.ToObjectReturnType.ItemsListFieldType> getItemsList();

        @JsProperty
        void setCanonicalPath(String canonicalPath);

        @JsOverlay
        default void setItemsList(ListItemsResponse.ToObjectReturnType.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<ListItemsResponse.ToObjectReturnType.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(JsArray<ListItemsResponse.ToObjectReturnType.ItemsListFieldType> itemsList);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ItemsListFieldType {
            @JsOverlay
            static ListItemsResponse.ToObjectReturnType0.ItemsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getEtag();

            @JsProperty
            String getPath();

            @JsProperty
            String getSize();

            @JsProperty
            double getType();

            @JsProperty
            void setEtag(String etag);

            @JsProperty
            void setPath(String path);

            @JsProperty
            void setSize(String size);

            @JsProperty
            void setType(double type);
        }

        @JsOverlay
        static ListItemsResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getCanonicalPath();

        @JsProperty
        JsArray<ListItemsResponse.ToObjectReturnType0.ItemsListFieldType> getItemsList();

        @JsProperty
        void setCanonicalPath(String canonicalPath);

        @JsOverlay
        default void setItemsList(
                ListItemsResponse.ToObjectReturnType0.ItemsListFieldType[] itemsList) {
            setItemsList(
                    Js.<JsArray<ListItemsResponse.ToObjectReturnType0.ItemsListFieldType>>uncheckedCast(
                            itemsList));
        }

        @JsProperty
        void setItemsList(JsArray<ListItemsResponse.ToObjectReturnType0.ItemsListFieldType> itemsList);
    }

    public static native ListItemsResponse deserializeBinary(Uint8Array bytes);

    public static native ListItemsResponse deserializeBinaryFromReader(
            ListItemsResponse message, Object reader);

    public static native void serializeBinaryToWriter(ListItemsResponse message, Object writer);

    public static native ListItemsResponse.ToObjectReturnType toObject(
            boolean includeInstance, ListItemsResponse msg);

    public native ItemInfo addItems();

    public native ItemInfo addItems(ItemInfo value, double index);

    public native ItemInfo addItems(ItemInfo value);

    public native void clearItemsList();

    public native String getCanonicalPath();

    public native JsArray<ItemInfo> getItemsList();

    public native Uint8Array serializeBinary();

    public native void setCanonicalPath(String value);

    @JsOverlay
    public final void setItemsList(ItemInfo[] value) {
        setItemsList(Js.<JsArray<ItemInfo>>uncheckedCast(value));
    }

    public native void setItemsList(JsArray<ItemInfo> value);

    public native ListItemsResponse.ToObjectReturnType0 toObject();

    public native ListItemsResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
