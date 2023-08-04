package io.deephaven.web.client.api;

public class DatabarFormatBuilder {
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

    public DatabarFormatBuilder setMin(double min) {
        this.min = min;
        return this;
    }

    public DatabarFormatBuilder setMax(double max) {
        this.max = max;
        return this;
    }

    public DatabarFormatBuilder setValue(double value) {
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

    public DatabarFormatBuilder setOpacity(double opacity) {
        this.opacity = opacity;
        return this;
    }

    public DatabarFormatBuilder setMarker(double marker) {
        this.marker = marker;
        return this;
    }

    public DatabarFormatBuilder setMarkerColor(String markerColor) {
        this.markerColor = markerColor;
        return this;
    }

    public DataBarFormat build() {
        return new DataBarFormat(min, max, value, axis, positiveColor, negativeColor, valuePlacement, direction,
                opacity, marker, markerColor);
    }
}
