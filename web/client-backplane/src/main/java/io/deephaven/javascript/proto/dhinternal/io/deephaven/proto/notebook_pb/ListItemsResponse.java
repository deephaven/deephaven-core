package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb;

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
        name = "dhinternal.io.deephaven.proto.notebook_pb.ListItemsResponse",
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
            double getKind();

            @JsProperty
            String getPath();

            @JsProperty
            String getSize();

            @JsProperty
            void setKind(double kind);

            @JsProperty
            void setPath(String path);

            @JsProperty
            void setSize(String size);
        }

        @JsOverlay
        static ListItemsResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ListItemsResponse.ToObjectReturnType.ItemsListFieldType> getItemsList();

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
            double getKind();

            @JsProperty
            String getPath();

            @JsProperty
            String getSize();

            @JsProperty
            void setKind(double kind);

            @JsProperty
            void setPath(String path);

            @JsProperty
            void setSize(String size);
        }

        @JsOverlay
        static ListItemsResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ListItemsResponse.ToObjectReturnType0.ItemsListFieldType> getItemsList();

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

    public native FileInfo addItems();

    public native FileInfo addItems(FileInfo value, double index);

    public native FileInfo addItems(FileInfo value);

    public native void clearItemsList();

    public native JsArray<FileInfo> getItemsList();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setItemsList(FileInfo[] value) {
        setItemsList(Js.<JsArray<FileInfo>>uncheckedCast(value));
    }

    public native void setItemsList(JsArray<FileInfo> value);

    public native ListItemsResponse.ToObjectReturnType0 toObject();

    public native ListItemsResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
