package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsProperty;
import java.util.Optional;

@TsInterface
@TsName(namespace = "dh")
public class DataBarFormat {
    private final double min;
    private final double max;
    private final double value;
    private final String axis;
    private final String positiveColor;
    private final String negativeColor;
    private final String valuePlacement;
    private final String direction;
    private final double opacity;
    private final double marker;
    private final String markerColor;

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


