package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.runchartdownsamplerequest.ZoomRange;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.RunChartDownsampleRequest",
        namespace = JsPackage.GLOBAL)
public class RunChartDownsampleRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType of(
                        Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ZoomRangeFieldType {
            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType.ZoomRangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getMaxDateNanos();

            @JsProperty
            String getMinDateNanos();

            @JsProperty
            void setMaxDateNanos(String maxDateNanos);

            @JsProperty
            void setMinDateNanos(String minDateNanos);
        }

        @JsOverlay
        static RunChartDownsampleRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPixelCount();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType getResultId();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

        @JsProperty
        String getXColumnName();

        @JsProperty
        JsArray<String> getYColumnNamesList();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType.ZoomRangeFieldType getZoomRange();

        @JsProperty
        void setPixelCount(double pixelCount);

        @JsProperty
        void setResultId(RunChartDownsampleRequest.ToObjectReturnType.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(RunChartDownsampleRequest.ToObjectReturnType.SourceIdFieldType sourceId);

        @JsProperty
        void setXColumnName(String xColumnName);

        @JsProperty
        void setYColumnNamesList(JsArray<String> yColumnNamesList);

        @JsOverlay
        default void setYColumnNamesList(String[] yColumnNamesList) {
            setYColumnNamesList(Js.<JsArray<String>>uncheckedCast(yColumnNamesList));
        }

        @JsProperty
        void setZoomRange(RunChartDownsampleRequest.ToObjectReturnType.ZoomRangeFieldType zoomRange);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
                @JsOverlay
                static RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType of(Object o) {
                    return Js.cast(o);
                }

                @JsOverlay
                default String asString() {
                    return Js.asString(this);
                }

                @JsOverlay
                default Uint8Array asUint8Array() {
                    return Js.cast(this);
                }

                @JsOverlay
                default boolean isString() {
                    return (Object) this instanceof String;
                }

                @JsOverlay
                default boolean isUint8Array() {
                    return (Object) this instanceof Uint8Array;
                }
            }

            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType getTicket();

            @JsProperty
            void setTicket(
                    RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType ticket);

            @JsOverlay
            default void setTicket(String ticket) {
                setTicket(
                        Js.<RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
                setTicket(
                        Js.<RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType.GetTicketUnionType>uncheckedCast(
                                ticket));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SourceIdFieldType {
            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType0.SourceIdFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getBatchOffset();

            @JsProperty
            Object getTicket();

            @JsProperty
            void setBatchOffset(double batchOffset);

            @JsProperty
            void setTicket(Object ticket);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ZoomRangeFieldType {
            @JsOverlay
            static RunChartDownsampleRequest.ToObjectReturnType0.ZoomRangeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getMaxDateNanos();

            @JsProperty
            String getMinDateNanos();

            @JsProperty
            void setMaxDateNanos(String maxDateNanos);

            @JsProperty
            void setMinDateNanos(String minDateNanos);
        }

        @JsOverlay
        static RunChartDownsampleRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPixelCount();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

        @JsProperty
        String getXColumnName();

        @JsProperty
        JsArray<String> getYColumnNamesList();

        @JsProperty
        RunChartDownsampleRequest.ToObjectReturnType0.ZoomRangeFieldType getZoomRange();

        @JsProperty
        void setPixelCount(double pixelCount);

        @JsProperty
        void setResultId(RunChartDownsampleRequest.ToObjectReturnType0.ResultIdFieldType resultId);

        @JsProperty
        void setSourceId(RunChartDownsampleRequest.ToObjectReturnType0.SourceIdFieldType sourceId);

        @JsProperty
        void setXColumnName(String xColumnName);

        @JsProperty
        void setYColumnNamesList(JsArray<String> yColumnNamesList);

        @JsOverlay
        default void setYColumnNamesList(String[] yColumnNamesList) {
            setYColumnNamesList(Js.<JsArray<String>>uncheckedCast(yColumnNamesList));
        }

        @JsProperty
        void setZoomRange(RunChartDownsampleRequest.ToObjectReturnType0.ZoomRangeFieldType zoomRange);
    }

    public static native RunChartDownsampleRequest deserializeBinary(Uint8Array bytes);

    public static native RunChartDownsampleRequest deserializeBinaryFromReader(
            RunChartDownsampleRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            RunChartDownsampleRequest message, Object writer);

    public static native RunChartDownsampleRequest.ToObjectReturnType toObject(
            boolean includeInstance, RunChartDownsampleRequest msg);

    public native String addYColumnNames(String value, double index);

    public native String addYColumnNames(String value);

    public native void clearResultId();

    public native void clearSourceId();

    public native void clearYColumnNamesList();

    public native void clearZoomRange();

    public native int getPixelCount();

    public native Ticket getResultId();

    public native TableReference getSourceId();

    public native String getXColumnName();

    public native JsArray<String> getYColumnNamesList();

    public native ZoomRange getZoomRange();

    public native boolean hasResultId();

    public native boolean hasSourceId();

    public native boolean hasZoomRange();

    public native Uint8Array serializeBinary();

    public native void setPixelCount(int value);

    public native void setResultId();

    public native void setResultId(Ticket value);

    public native void setSourceId();

    public native void setSourceId(TableReference value);

    public native void setXColumnName(String value);

    public native void setYColumnNamesList(JsArray<String> value);

    @JsOverlay
    public final void setYColumnNamesList(String[] value) {
        setYColumnNamesList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setZoomRange();

    public native void setZoomRange(ZoomRange value);

    public native RunChartDownsampleRequest.ToObjectReturnType0 toObject();

    public native RunChartDownsampleRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
