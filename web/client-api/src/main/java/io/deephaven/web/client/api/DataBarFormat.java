package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsProperty;

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

    public DataBarFormat(Double min, Double max, Double value, String axis, String positiveColor, String negativeColor, String valuePlacement, String direction, Double opacity) {
        this.min = min;
        this.max = max;
        this.value = value;
        this.axis = axis;
        this.positiveColor = positiveColor;
        this.negativeColor = negativeColor;
        this.valuePlacement = valuePlacement;
        this.direction = direction;
        this.opacity = opacity;
    }

    @JsProperty
    public double getMin() { return min; }
    @JsProperty
    public double getMax() { return max; }
    @JsProperty
    public double getValue() { return value; }
    @JsProperty
    public String getAxis() { return axis; }
    @JsProperty
    public String getPositiveColor() { return positiveColor; }

    @JsProperty
    public String getNegativeColor() { return negativeColor; }
    @JsProperty
    public String getValuePlacement() { return valuePlacement; }
    @JsProperty
    public String getDirection() { return direction; }
    @JsProperty
    public double getOpacity() { return opacity; }
}


