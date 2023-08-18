package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.barrage.DatabarFormatColumnType;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.Map;

@TsInterface
@TsName(namespace = "dh")
public class DataBarFormat {
    private double min;
    private double max;
    private double value;
    private String axis;
    private String positiveColor;
    private String negativeColor;
    private String valuePlacement;
    private String direction;
    private double opacity;
    private double marker;
    private String markerColor;

    public DataBarFormat(double min, double max, double value, String axis, String positiveColor, String negativeColor,
            String valuePlacement, String direction, double opacity, double marker, String markerColor) {
        this.min = min;
        this.max = max;
        this.value = value;
        this.axis = axis;
        this.positiveColor = positiveColor;
        this.negativeColor = negativeColor;
        this.valuePlacement = valuePlacement;
        this.direction = direction;
        this.opacity = opacity;
        this.marker = marker;
        this.markerColor = markerColor;
    }

    public DataBarFormat(Map<String, Integer> dataBarColumnIndices, Object[] dataColumns, int offsetInSnapshot) {
        for (DatabarFormatColumnType type : DatabarFormatColumnType.values()) {
            int index = dataBarColumnIndices.get(type.name());
            JsArray<Any> val = Js.uncheckedCast(dataColumns[index]);

            switch (type) {
                case MIN:
                    this.min = val.getAtAsAny(offsetInSnapshot).asDouble();
                    break;
                case MAX:
                    this.max = val.getAtAsAny(offsetInSnapshot).asDouble();
                    break;
                case VALUE:
                    this.value = val.getAtAsAny(offsetInSnapshot).asDouble();
                    break;
                case AXIS:
                    this.axis = val.getAtAsAny(offsetInSnapshot).asString();
                    break;
                case POSITIVE_COLOR:
                    if (val.getAtAsAny(offsetInSnapshot) != null) {
                        this.positiveColor = val.getAtAsAny(offsetInSnapshot).asString();
                    }
                    break;
                case NEGATIVE_COLOR:
                    if (val.getAtAsAny(offsetInSnapshot) != null) {
                        this.negativeColor = val.getAtAsAny(offsetInSnapshot).asString();
                    }
                    break;
                case VALUE_PLACEMENT:
                    this.valuePlacement = val.getAtAsAny(offsetInSnapshot).asString();
                    break;
                case DIRECTION:
                    this.direction = val.getAtAsAny(offsetInSnapshot).asString();
                    break;
                case OPACITY:
                    this.opacity = val.getAtAsAny(offsetInSnapshot).asDouble();
                    break;
                case MARKER:
                    if (val.getAtAsAny(offsetInSnapshot) != null) {
                        this.marker = val.getAtAsAny(offsetInSnapshot).asDouble();
                    }
                    break;
                case MARKER_COLOR:
                    if (val.getAtAsAny(offsetInSnapshot) != null) {
                        this.markerColor = val.getAtAsAny(offsetInSnapshot).asString();
                    }
                    break;
                default:
                    throw new RuntimeException("No column index was found for this data bar type: " + type);
            }
        }
    }

    @JsProperty
    public double getMin() {
        return min;
    }

    @JsProperty
    public double getMax() {
        return max;
    }

    @JsProperty
    public double getValue() {
        return value;
    }

    @JsProperty
    public String getAxis() {
        return axis;
    }

    @JsProperty
    public String getPositiveColor() {
        return positiveColor;
    }

    @JsProperty
    public String getNegativeColor() {
        return negativeColor;
    }

    @JsProperty
    public String getValuePlacement() {
        return valuePlacement;
    }

    @JsProperty
    public String getDirection() {
        return direction;
    }

    @JsProperty
    public double getOpacity() {
        return opacity;
    }

    @JsProperty
    public double getMarker() {
        return marker;
    }

    @JsProperty
    public String getMarkerColor() {
        return markerColor;
    }
}


