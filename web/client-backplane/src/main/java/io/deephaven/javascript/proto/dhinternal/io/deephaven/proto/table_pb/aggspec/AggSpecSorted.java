package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortDescriptor;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AggSpec.AggSpecSorted",
        namespace = JsPackage.GLOBAL)
public class AggSpecSorted {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnsListFieldType {
            @JsOverlay
            static AggSpecSorted.ToObjectReturnType.ColumnsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            double getDirection();

            @JsProperty
            boolean isIsAbsolute();

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setDirection(double direction);

            @JsProperty
            void setIsAbsolute(boolean isAbsolute);
        }

        @JsOverlay
        static AggSpecSorted.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggSpecSorted.ToObjectReturnType.ColumnsListFieldType> getColumnsList();

        @JsOverlay
        default void setColumnsList(
                AggSpecSorted.ToObjectReturnType.ColumnsListFieldType[] columnsList) {
            setColumnsList(
                    Js.<JsArray<AggSpecSorted.ToObjectReturnType.ColumnsListFieldType>>uncheckedCast(
                            columnsList));
        }

        @JsProperty
        void setColumnsList(JsArray<AggSpecSorted.ToObjectReturnType.ColumnsListFieldType> columnsList);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ColumnsListFieldType {
            @JsOverlay
            static AggSpecSorted.ToObjectReturnType0.ColumnsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            double getDirection();

            @JsProperty
            boolean isIsAbsolute();

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setDirection(double direction);

            @JsProperty
            void setIsAbsolute(boolean isAbsolute);
        }

        @JsOverlay
        static AggSpecSorted.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<AggSpecSorted.ToObjectReturnType0.ColumnsListFieldType> getColumnsList();

        @JsOverlay
        default void setColumnsList(
                AggSpecSorted.ToObjectReturnType0.ColumnsListFieldType[] columnsList) {
            setColumnsList(
                    Js.<JsArray<AggSpecSorted.ToObjectReturnType0.ColumnsListFieldType>>uncheckedCast(
                            columnsList));
        }

        @JsProperty
        void setColumnsList(
                JsArray<AggSpecSorted.ToObjectReturnType0.ColumnsListFieldType> columnsList);
    }

    public static native AggSpecSorted deserializeBinary(Uint8Array bytes);

    public static native AggSpecSorted deserializeBinaryFromReader(
            AggSpecSorted message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecSorted message, Object writer);

    public static native AggSpecSorted.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecSorted msg);

    public native SortDescriptor addColumns();

    public native SortDescriptor addColumns(SortDescriptor value, double index);

    public native SortDescriptor addColumns(SortDescriptor value);

    public native void clearColumnsList();

    public native JsArray<SortDescriptor> getColumnsList();

    public native Uint8Array serializeBinary();

    public native void setColumnsList(JsArray<SortDescriptor> value);

    @JsOverlay
    public final void setColumnsList(SortDescriptor[] value) {
        setColumnsList(Js.<JsArray<SortDescriptor>>uncheckedCast(value));
    }

    public native AggSpecSorted.ToObjectReturnType0 toObject();

    public native AggSpecSorted.ToObjectReturnType0 toObject(boolean includeInstance);
}
