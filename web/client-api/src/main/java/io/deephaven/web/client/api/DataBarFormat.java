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
    private Double min;
    private Double max;
    private Double value;
    private String axis;
    private String positiveColor;
    private String negativeColor;
    private String valuePlacement;
    private String direction;
    private Double opacity;
    private Double marker;
    private String markerColor;

    public DataBarFormat(Map<String, Integer> dataBarColumnIndices, Object[] dataColumns, int offsetInSnapshot) {
        for (DatabarFormatColumnType type : DatabarFormatColumnType.values()) {
            int index = dataBarColumnIndices.get(type.name());
            JsArray<Any> val = Js.uncheckedCast(dataColumns[index]);

            if(val.getAtAsAny(offsetInSnapshot) != null) {
                switch (type) {
                    case MIN:
                        this.min = Js.coerceToDouble(val.getAtAsAny(offsetInSnapshot));
                        break;
                    case MAX:
                        this.max = Js.coerceToDouble(val.getAtAsAny(offsetInSnapshot));
                        break;
                    case VALUE:
                        this.value = Js.coerceToDouble(val.getAtAsAny(offsetInSnapshot));
                        break;
                    case AXIS:
                        this.axis = val.getAtAsAny(offsetInSnapshot).asString();
                        break;
                    case POSITIVE_COLOR:
                        this.positiveColor = val.getAtAsAny(offsetInSnapshot).asString();
                        break;
                    case NEGATIVE_COLOR:
                        this.negativeColor = val.getAtAsAny(offsetInSnapshot).asString();
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
                        this.marker = Js.coerceToDouble(val.getAtAsAny(offsetInSnapshot));
                        break;
                    case MARKER_COLOR:
                        this.markerColor = val.getAtAsAny(offsetInSnapshot).asString();
                        break;
                    default:
                        throw new RuntimeException("No column index was found for this data bar type: " + type);
                }
            }
        }
    }
    @JsProperty
    public Double getMin() {
        return min;
    }

    @JsProperty
    public Double getMax() {
        return max;
    }

    @JsProperty
    public Double getValue() {
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
    public Double getOpacity() {
        return opacity;
    }

    @JsProperty
    public Double getMarker() {
        return marker;
    }

    @JsProperty
    public String getMarkerColor() {
        return markerColor;
    }
}


