package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

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
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SourceDescriptor",
        namespace = JsPackage.GLOBAL)
public class SourceDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OneClickFieldType {
            @JsOverlay
            static SourceDescriptor.ToObjectReturnType.OneClickFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getColumnTypesList();

            @JsProperty
            JsArray<String> getColumnsList();

            @JsProperty
            boolean isRequireAllFiltersToDisplay();

            @JsProperty
            void setColumnTypesList(JsArray<String> columnTypesList);

            @JsOverlay
            default void setColumnTypesList(String[] columnTypesList) {
                setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
            }

            @JsProperty
            void setColumnsList(JsArray<String> columnsList);

            @JsOverlay
            default void setColumnsList(String[] columnsList) {
                setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
            }

            @JsProperty
            void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
        }

        @JsOverlay
        static SourceDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getAxisId();

        @JsProperty
        String getColumnName();

        @JsProperty
        String getColumnType();

        @JsProperty
        SourceDescriptor.ToObjectReturnType.OneClickFieldType getOneClick();

        @JsProperty
        double getTableId();

        @JsProperty
        double getTableMapId();

        @JsProperty
        double getType();

        @JsProperty
        void setAxisId(String axisId);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setColumnType(String columnType);

        @JsProperty
        void setOneClick(SourceDescriptor.ToObjectReturnType.OneClickFieldType oneClick);

        @JsProperty
        void setTableId(double tableId);

        @JsProperty
        void setTableMapId(double tableMapId);

        @JsProperty
        void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OneClickFieldType {
            @JsOverlay
            static SourceDescriptor.ToObjectReturnType0.OneClickFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getColumnTypesList();

            @JsProperty
            JsArray<String> getColumnsList();

            @JsProperty
            boolean isRequireAllFiltersToDisplay();

            @JsProperty
            void setColumnTypesList(JsArray<String> columnTypesList);

            @JsOverlay
            default void setColumnTypesList(String[] columnTypesList) {
                setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
            }

            @JsProperty
            void setColumnsList(JsArray<String> columnsList);

            @JsOverlay
            default void setColumnsList(String[] columnsList) {
                setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
            }

            @JsProperty
            void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
        }

        @JsOverlay
        static SourceDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getAxisId();

        @JsProperty
        String getColumnName();

        @JsProperty
        String getColumnType();

        @JsProperty
        SourceDescriptor.ToObjectReturnType0.OneClickFieldType getOneClick();

        @JsProperty
        double getTableId();

        @JsProperty
        double getTableMapId();

        @JsProperty
        double getType();

        @JsProperty
        void setAxisId(String axisId);

        @JsProperty
        void setColumnName(String columnName);

        @JsProperty
        void setColumnType(String columnType);

        @JsProperty
        void setOneClick(SourceDescriptor.ToObjectReturnType0.OneClickFieldType oneClick);

        @JsProperty
        void setTableId(double tableId);

        @JsProperty
        void setTableMapId(double tableMapId);

        @JsProperty
        void setType(double type);
    }

    public static native SourceDescriptor deserializeBinary(Uint8Array bytes);

    public static native SourceDescriptor deserializeBinaryFromReader(
            SourceDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(SourceDescriptor message, Object writer);

    public static native SourceDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, SourceDescriptor msg);

    public native void clearOneClick();

    public native String getAxisId();

    public native String getColumnName();

    public native String getColumnType();

    public native OneClickDescriptor getOneClick();

    public native int getTableId();

    public native int getTableMapId();

    public native int getType();

    public native boolean hasOneClick();

    public native Uint8Array serializeBinary();

    public native void setAxisId(String value);

    public native void setColumnName(String value);

    public native void setColumnType(String value);

    public native void setOneClick();

    public native void setOneClick(OneClickDescriptor value);

    public native void setTableId(int value);

    public native void setTableMapId(int value);

    public native void setType(int value);

    public native SourceDescriptor.ToObjectReturnType0 toObject();

    public native SourceDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
