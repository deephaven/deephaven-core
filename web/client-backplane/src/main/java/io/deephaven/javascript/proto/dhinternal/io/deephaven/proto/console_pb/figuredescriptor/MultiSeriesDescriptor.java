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
        name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.MultiSeriesDescriptor",
        namespace = JsPackage.GLOBAL)
public class MultiSeriesDescriptor {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataSourcesListFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LineColorFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType.LineColorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getDefaultString();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<String> getValuesList();

            @JsProperty
            void setDefaultString(String defaultString);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<String> valuesList);

            @JsOverlay
            default void setValuesList(String[] valuesList) {
                setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LinesVisibleFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType.LinesVisibleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Boolean> getValuesList();

            @JsProperty
            boolean isDefaultBool();

            @JsProperty
            void setDefaultBool(boolean defaultBool);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<Boolean> valuesList);

            @JsOverlay
            default void setValuesList(boolean[] valuesList) {
                setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PointSizeFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType.PointSizeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDefaultDouble();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Double> getValuesList();

            @JsProperty
            void setDefaultDouble(double defaultDouble);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<Double> valuesList);

            @JsOverlay
            default void setValuesList(double[] valuesList) {
                setValuesList(Js.<JsArray<Double>>uncheckedCast(valuesList));
            }
        }

        @JsOverlay
        static MultiSeriesDescriptor.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<MultiSeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType> getDataSourcesList();

        @JsProperty
        Object getGradientVisible();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType.LineColorFieldType getLineColor();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType.LinesVisibleFieldType getLinesVisible();

        @JsProperty
        String getName();

        @JsProperty
        double getPlotStyle();

        @JsProperty
        Object getPointColor();

        @JsProperty
        Object getPointLabel();

        @JsProperty
        Object getPointLabelFormat();

        @JsProperty
        Object getPointShape();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType.PointSizeFieldType getPointSize();

        @JsProperty
        Object getPointsVisible();

        @JsProperty
        Object getXToolTipPattern();

        @JsProperty
        Object getYToolTipPattern();

        @JsOverlay
        default void setDataSourcesList(
                MultiSeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType[] dataSourcesList) {
            setDataSourcesList(
                    Js.<JsArray<MultiSeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType>>uncheckedCast(
                            dataSourcesList));
        }

        @JsProperty
        void setDataSourcesList(
                JsArray<MultiSeriesDescriptor.ToObjectReturnType.DataSourcesListFieldType> dataSourcesList);

        @JsProperty
        void setGradientVisible(Object gradientVisible);

        @JsProperty
        void setLineColor(MultiSeriesDescriptor.ToObjectReturnType.LineColorFieldType lineColor);

        @JsProperty
        void setLinesVisible(
                MultiSeriesDescriptor.ToObjectReturnType.LinesVisibleFieldType linesVisible);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setPlotStyle(double plotStyle);

        @JsProperty
        void setPointColor(Object pointColor);

        @JsProperty
        void setPointLabel(Object pointLabel);

        @JsProperty
        void setPointLabelFormat(Object pointLabelFormat);

        @JsProperty
        void setPointShape(Object pointShape);

        @JsProperty
        void setPointSize(MultiSeriesDescriptor.ToObjectReturnType.PointSizeFieldType pointSize);

        @JsProperty
        void setPointsVisible(Object pointsVisible);

        @JsProperty
        void setXToolTipPattern(Object xToolTipPattern);

        @JsProperty
        void setYToolTipPattern(Object yToolTipPattern);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DataSourcesListFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LineColorFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType0.LineColorFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getDefaultString();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<String> getValuesList();

            @JsProperty
            void setDefaultString(String defaultString);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<String> valuesList);

            @JsOverlay
            default void setValuesList(String[] valuesList) {
                setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface LinesVisibleFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType0.LinesVisibleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Boolean> getValuesList();

            @JsProperty
            boolean isDefaultBool();

            @JsProperty
            void setDefaultBool(boolean defaultBool);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<Boolean> valuesList);

            @JsOverlay
            default void setValuesList(boolean[] valuesList) {
                setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
            }
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface PointSizeFieldType {
            @JsOverlay
            static MultiSeriesDescriptor.ToObjectReturnType0.PointSizeFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDefaultDouble();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Double> getValuesList();

            @JsProperty
            void setDefaultDouble(double defaultDouble);

            @JsProperty
            void setKeysList(JsArray<String> keysList);

            @JsOverlay
            default void setKeysList(String[] keysList) {
                setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
            }

            @JsProperty
            void setValuesList(JsArray<Double> valuesList);

            @JsOverlay
            default void setValuesList(double[] valuesList) {
                setValuesList(Js.<JsArray<Double>>uncheckedCast(valuesList));
            }
        }

        @JsOverlay
        static MultiSeriesDescriptor.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType> getDataSourcesList();

        @JsProperty
        Object getGradientVisible();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType0.LineColorFieldType getLineColor();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType0.LinesVisibleFieldType getLinesVisible();

        @JsProperty
        String getName();

        @JsProperty
        double getPlotStyle();

        @JsProperty
        Object getPointColor();

        @JsProperty
        Object getPointLabel();

        @JsProperty
        Object getPointLabelFormat();

        @JsProperty
        Object getPointShape();

        @JsProperty
        MultiSeriesDescriptor.ToObjectReturnType0.PointSizeFieldType getPointSize();

        @JsProperty
        Object getPointsVisible();

        @JsProperty
        Object getXToolTipPattern();

        @JsProperty
        Object getYToolTipPattern();

        @JsOverlay
        default void setDataSourcesList(
                MultiSeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType[] dataSourcesList) {
            setDataSourcesList(
                    Js.<JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType>>uncheckedCast(
                            dataSourcesList));
        }

        @JsProperty
        void setDataSourcesList(
                JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DataSourcesListFieldType> dataSourcesList);

        @JsProperty
        void setGradientVisible(Object gradientVisible);

        @JsProperty
        void setLineColor(MultiSeriesDescriptor.ToObjectReturnType0.LineColorFieldType lineColor);

        @JsProperty
        void setLinesVisible(
                MultiSeriesDescriptor.ToObjectReturnType0.LinesVisibleFieldType linesVisible);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setPlotStyle(double plotStyle);

        @JsProperty
        void setPointColor(Object pointColor);

        @JsProperty
        void setPointLabel(Object pointLabel);

        @JsProperty
        void setPointLabelFormat(Object pointLabelFormat);

        @JsProperty
        void setPointShape(Object pointShape);

        @JsProperty
        void setPointSize(MultiSeriesDescriptor.ToObjectReturnType0.PointSizeFieldType pointSize);

        @JsProperty
        void setPointsVisible(Object pointsVisible);

        @JsProperty
        void setXToolTipPattern(Object xToolTipPattern);

        @JsProperty
        void setYToolTipPattern(Object yToolTipPattern);
    }

    public static native MultiSeriesDescriptor deserializeBinary(Uint8Array bytes);

    public static native MultiSeriesDescriptor deserializeBinaryFromReader(
            MultiSeriesDescriptor message, Object reader);

    public static native void serializeBinaryToWriter(MultiSeriesDescriptor message, Object writer);

    public static native MultiSeriesDescriptor.ToObjectReturnType toObject(
            boolean includeInstance, MultiSeriesDescriptor msg);

    public native MultiSeriesSourceDescriptor addDataSources();

    public native MultiSeriesSourceDescriptor addDataSources(
            MultiSeriesSourceDescriptor value, double index);

    public native MultiSeriesSourceDescriptor addDataSources(MultiSeriesSourceDescriptor value);

    public native void clearDataSourcesList();

    public native void clearGradientVisible();

    public native void clearLineColor();

    public native void clearLinesVisible();

    public native void clearPointColor();

    public native void clearPointLabel();

    public native void clearPointLabelFormat();

    public native void clearPointShape();

    public native void clearPointSize();

    public native void clearPointsVisible();

    public native void clearXToolTipPattern();

    public native void clearYToolTipPattern();

    public native JsArray<MultiSeriesSourceDescriptor> getDataSourcesList();

    public native BoolMapWithDefault getGradientVisible();

    public native StringMapWithDefault getLineColor();

    public native BoolMapWithDefault getLinesVisible();

    public native String getName();

    public native int getPlotStyle();

    public native StringMapWithDefault getPointColor();

    public native StringMapWithDefault getPointLabel();

    public native StringMapWithDefault getPointLabelFormat();

    public native StringMapWithDefault getPointShape();

    public native DoubleMapWithDefault getPointSize();

    public native BoolMapWithDefault getPointsVisible();

    public native StringMapWithDefault getXToolTipPattern();

    public native StringMapWithDefault getYToolTipPattern();

    public native boolean hasGradientVisible();

    public native boolean hasLineColor();

    public native boolean hasLinesVisible();

    public native boolean hasPointColor();

    public native boolean hasPointLabel();

    public native boolean hasPointLabelFormat();

    public native boolean hasPointShape();

    public native boolean hasPointSize();

    public native boolean hasPointsVisible();

    public native boolean hasXToolTipPattern();

    public native boolean hasYToolTipPattern();

    public native Uint8Array serializeBinary();

    public native void setDataSourcesList(JsArray<MultiSeriesSourceDescriptor> value);

    @JsOverlay
    public final void setDataSourcesList(MultiSeriesSourceDescriptor[] value) {
        setDataSourcesList(Js.<JsArray<MultiSeriesSourceDescriptor>>uncheckedCast(value));
    }

    public native void setGradientVisible();

    public native void setGradientVisible(BoolMapWithDefault value);

    public native void setLineColor();

    public native void setLineColor(StringMapWithDefault value);

    public native void setLinesVisible();

    public native void setLinesVisible(BoolMapWithDefault value);

    public native void setName(String value);

    public native void setPlotStyle(int value);

    public native void setPointColor();

    public native void setPointColor(StringMapWithDefault value);

    public native void setPointLabel();

    public native void setPointLabel(StringMapWithDefault value);

    public native void setPointLabelFormat();

    public native void setPointLabelFormat(StringMapWithDefault value);

    public native void setPointShape();

    public native void setPointShape(StringMapWithDefault value);

    public native void setPointSize();

    public native void setPointSize(DoubleMapWithDefault value);

    public native void setPointsVisible();

    public native void setPointsVisible(BoolMapWithDefault value);

    public native void setXToolTipPattern();

    public native void setXToolTipPattern(StringMapWithDefault value);

    public native void setYToolTipPattern();

    public native void setYToolTipPattern(StringMapWithDefault value);

    public native MultiSeriesDescriptor.ToObjectReturnType0 toObject();

    public native MultiSeriesDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
