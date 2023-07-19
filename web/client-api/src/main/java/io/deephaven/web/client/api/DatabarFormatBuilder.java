package io.deephaven.web.client.api;

public class DatabarFormatBuilder {
    private Double min;
    private Double max;
    private Double value;
    private String axis;
    private String positiveColor;
    private String negativeColor;
    private String valuePlacement;
    private String direction;
    private Double opacity;

    public DatabarFormatBuilder setMin(Double min) {
        this.min = min;
        return this;
    }

    public DatabarFormatBuilder setMax(Double max) {
        this.max = max;
        return this;
    }

    public DatabarFormatBuilder setValue(Double value) {
        this.value = value;
        return this;
    }

    public DatabarFormatBuilder setAxis(String axis) {
        this.axis = axis;
        return this;
    }

    public DatabarFormatBuilder setPositiveColor(String positiveColor) {
        this.positiveColor = positiveColor;
        return this;
    }

    public DatabarFormatBuilder setNegativeColor(String negativeColor) {
        this.negativeColor = negativeColor;
        return this;
    }

    public DatabarFormatBuilder setValuePlacement(String valuePlacement) {
        this.valuePlacement = valuePlacement;
        return this;
    }

    public DatabarFormatBuilder setDirection(String direction) {
        this.direction = direction;
        return this;
    }

    public DatabarFormatBuilder setOpacity(Double opacity) {
        this.opacity = opacity;
        return this;
    }

    public DataBarFormat build() {
        return new DataBarFormat(min, max, value, axis, positiveColor, negativeColor, valuePlacement, direction, opacity);
    }
}