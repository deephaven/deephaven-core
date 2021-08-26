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
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SeriesDescriptor",
        namespace = JsPackage.GLOBAL)
public class SeriesDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataSourcesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OneClickFieldType {
                @JsOverlay
                static SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType.OneClickFieldType create() {
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
            static SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            String getColumnType();

            @JsProperty
            SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType.OneClickFieldType getOneClick();

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
            void setOneClick(
                    SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType.OneClickFieldType oneClick);

            @JsProperty
            void setTableId(double tableId);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
        }

        @JsOverlay
        static SeriesDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType> getDataSourcesList();

        @JsProperty
        String getLineColor();

        @JsProperty
        String getName();

        @JsProperty
        double getPlotStyle();

        @JsProperty
        String getPointLabelFormat();

        @JsProperty
        String getShape();

        @JsProperty
        String getShapeColor();

        @JsProperty
        String getShapeLabel();

        @JsProperty
        double getShapeSize();

        @JsProperty
        String getXToolTipPattern();

        @JsProperty
        String getYToolTipPattern();

        @JsProperty
        boolean isGradientVisible();

        @JsProperty
        boolean isLinesVisible();

        @JsProperty
        boolean isShapesVisible();

        @JsOverlay
        default void setDataSourcesList(
                SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType[] dataSourcesList) {
            setDataSourcesList(
                    Js.<JsArray<SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType>>uncheckedCast(
                            dataSourcesList));
        }

        @JsProperty
        void setDataSourcesList(
                JsArray<SeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType> dataSourcesList);

        @JsProperty
        void setGradientVisible(boolean gradientVisible);

        @JsProperty
        void setLineColor(String lineColor);

        @JsProperty
        void setLinesVisible(boolean linesVisible);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setPlotStyle(double plotStyle);

        @JsProperty
        void setPointLabelFormat(String pointLabelFormat);

        @JsProperty
        void setShape(String shape);

        @JsProperty
        void setShapeColor(String shapeColor);

        @JsProperty
        void setShapeLabel(String shapeLabel);

        @JsProperty
        void setShapeSize(double shapeSize);

        @JsProperty
        void setShapesVisible(boolean shapesVisible);

        @JsProperty
        void setXToolTipPattern(String xToolTipPattern);

        @JsProperty
        void setYToolTipPattern(String yToolTipPattern);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataSourcesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OneClickFieldType {
                @JsOverlay
                static SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType.OneClickFieldType create() {
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
            static SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            String getColumnType();

            @JsProperty
            SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType.OneClickFieldType getOneClick();

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
            void setOneClick(
                    SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType.OneClickFieldType oneClick);

            @JsProperty
            void setTableId(double tableId);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
        }

        @JsOverlay
        static SeriesDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType> getDataSourcesList();

        @JsProperty
        String getLineColor();

        @JsProperty
        String getName();

        @JsProperty
        double getPlotStyle();

        @JsProperty
        String getPointLabelFormat();

        @JsProperty
        String getShape();

        @JsProperty
        String getShapeColor();

        @JsProperty
        String getShapeLabel();

        @JsProperty
        double getShapeSize();

        @JsProperty
        String getXToolTipPattern();

        @JsProperty
        String getYToolTipPattern();

        @JsProperty
        boolean isGradientVisible();

        @JsProperty
        boolean isLinesVisible();

        @JsProperty
        boolean isShapesVisible();

        @JsOverlay
        default void setDataSourcesList(
                SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType[] dataSourcesList) {
            setDataSourcesList(
                    Js.<JsArray<SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType>>uncheckedCast(
                            dataSourcesList));
        }

        @JsProperty
        void setDataSourcesList(
                JsArray<SeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType> dataSourcesList);

        @JsProperty
        void setGradientVisible(boolean gradientVisible);

        @JsProperty
        void setLineColor(String lineColor);

        @JsProperty
        void setLinesVisible(boolean linesVisible);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setPlotStyle(double plotStyle);

        @JsProperty
        void setPointLabelFormat(String pointLabelFormat);

        @JsProperty
        void setShape(String shape);

        @JsProperty
        void setShapeColor(String shapeColor);

        @JsProperty
        void setShapeLabel(String shapeLabel);

        @JsProperty
        void setShapeSize(double shapeSize);

        @JsProperty
        void setShapesVisible(boolean shapesVisible);

        @JsProperty
        void setXToolTipPattern(String xToolTipPattern);

        @JsProperty
        void setYToolTipPattern(String yToolTipPattern);
    }

    public static native SeriesDescriptor deserializeBinary(Uint8Array bytes);

    public static native SeriesDescriptor deserializeBinaryFromReader(
            SeriesDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(SeriesDescriptor message, Object writer);

    public static native SeriesDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, SeriesDescriptor msg);

    public native SourceDescriptor addDataSources();

    public native SourceDescriptor addDataSources(SourceDescriptor value, double index);

    public native SourceDescriptor addDataSources(SourceDescriptor value);

    public native void clearDataSourcesList();

    public native void clearLinesVisible();

    public native void clearPointLabelFormat();

    public native void clearShapeSize();

    public native void clearShapesVisible();

    public native void clearXToolTipPattern();

    public native void clearYToolTipPattern();

    public native JsArray<SourceDescriptor> getDataSourcesList();

    public native boolean getGradientVisible();

    public native String getLineColor();

    public native boolean getLinesVisible();

    public native String getName();

    public native int getPlotStyle();

    public native String getPointLabelFormat();

    public native String getShape();

    public native String getShapeColor();

    public native String getShapeLabel();

    public native double getShapeSize();

    public native boolean getShapesVisible();

    public native String getXToolTipPattern();

    public native String getYToolTipPattern();

    public native boolean hasLinesVisible();

    public native boolean hasPointLabelFormat();

    public native boolean hasShapeSize();

    public native boolean hasShapesVisible();

    public native boolean hasXToolTipPattern();

    public native boolean hasYToolTipPattern();

    public native Uint8Array serializeBinary();

    public native void setDataSourcesList(JsArray<SourceDescriptor> value);

    @JsOverlay
    public final void setDataSourcesList(SourceDescriptor[] value) {
        setDataSourcesList(Js.<JsArray<SourceDescriptor>>uncheckedCast(value));
    }

    public native void setGradientVisible(boolean value);

    public native void setLineColor(String value);

    public native void setLinesVisible(boolean value);

    public native void setName(String value);

    public native void setPlotStyle(int value);

    public native void setPointLabelFormat(String value);

    public native void setShape(String value);

    public native void setShapeColor(String value);

    public native void setShapeLabel(String value);

    public native void setShapeSize(double value);

    public native void setShapesVisible(boolean value);

    public native void setXToolTipPattern(String value);

    public native void setYToolTipPattern(String value);

    public native SeriesDescriptor.ToObjectReturnType0 toObject();

    public native SeriesDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
