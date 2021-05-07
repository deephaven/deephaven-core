package io.deephaven.web.shared.data.plot;

import io.deephaven.web.shared.data.BusinessCalendarDescriptor;

import java.io.Serializable;

public class AxisDescriptor implements Serializable {
    public enum AxisFormatType { CATEGORY, NUMBER }
    public enum AxisType { X, Y, SHAPE, SIZE, LABEL, COLOR }
    public enum AxisPosition { TOP, BOTTOM, LEFT, RIGHT, NONE }

    private String id;

    private AxisFormatType formatType;

    private AxisType type;

    private AxisPosition position;

    private boolean log = false;
    private String label;
    private String labelFont;
    private String ticksFont;
//    private String format;
    private String formatPattern;
    private String color;
    private double minRange = Double.NaN;
    private double maxRange = Double.NaN;
    private boolean minorTicksVisible = false;
    private boolean majorTicksVisible = true;
    private int minorTickCount = 0;
    private double gapBetweenMajorTicks = -1.0;
    private double[] majorTickLocations;
//    private String axisTransform;
    private double tickLabelAngle = 0.0;
    private boolean invert = false;
    private boolean isTimeAxis = false;
    private BusinessCalendarDescriptor businessCalendarDescriptor;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public AxisFormatType getFormatType() {
        return formatType;
    }

    public void setFormatType(AxisFormatType formatType) {
        this.formatType = formatType;
    }

    public AxisType getType() {
        return type;
    }

    public void setType(AxisType type) {
        this.type = type;
    }

    public AxisPosition getPosition() {
        return position;
    }

    public void setPosition(AxisPosition position) {
        this.position = position;
    }

    public boolean isLog() {
        return log;
    }

    public void setLog(boolean log) {
        this.log = log;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabelFont() {
        return labelFont;
    }

    public void setLabelFont(String labelFont) {
        this.labelFont = labelFont;
    }

    public String getTicksFont() {
        return ticksFont;
    }

    public void setTicksFont(String ticksFont) {
        this.ticksFont = ticksFont;
    }

//    public String getFormat() {
//        return format;
//    }

//    public void setFormat(String format) {
//        this.format = format;
//    }

    public String getFormatPattern() {
        return formatPattern;
    }

    public void setFormatPattern(String formatPattern) {
        this.formatPattern = formatPattern;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public double getMinRange() {
        return minRange;
    }

    public void setMinRange(double minRange) {
        this.minRange = minRange;
    }

    public double getMaxRange() {
        return maxRange;
    }

    public void setMaxRange(double maxRange) {
        this.maxRange = maxRange;
    }

    public boolean isMinorTicksVisible() {
        return minorTicksVisible;
    }

    public void setMinorTicksVisible(boolean minorTicksVisible) {
        this.minorTicksVisible = minorTicksVisible;
    }

    public boolean isMajorTicksVisible() {
        return majorTicksVisible;
    }

    public void setMajorTicksVisible(boolean majorTicksVisible) {
        this.majorTicksVisible = majorTicksVisible;
    }

    public int getMinorTickCount() {
        return minorTickCount;
    }

    public void setMinorTickCount(int minorTickCount) {
        this.minorTickCount = minorTickCount;
    }

    public double getGapBetweenMajorTicks() {
        return gapBetweenMajorTicks;
    }

    public void setGapBetweenMajorTicks(double gapBetweenMajorTicks) {
        this.gapBetweenMajorTicks = gapBetweenMajorTicks;
    }

    public double[] getMajorTickLocations() {
        return majorTickLocations;
    }

    public void setMajorTickLocations(double[] majorTickLocations) {
        this.majorTickLocations = majorTickLocations;
    }

//    public String getAxisTransform() {
//        return axisTransform;
//    }
//
//    public void setAxisTransform(String axisTransform) {
//        this.axisTransform = axisTransform;
//    }

    public double getTickLabelAngle() {
        return tickLabelAngle;
    }

    public void setTickLabelAngle(double tickLabelAngle) {
        this.tickLabelAngle = tickLabelAngle;
    }

    public boolean isInvert() {
        return invert;
    }

    public void setInvert(boolean invert) {
        this.invert = invert;
    }

    public boolean isTimeAxis() {
        return isTimeAxis;
    }

    public void setTimeAxis(boolean timeAxis) {
        isTimeAxis = timeAxis;
    }

    public BusinessCalendarDescriptor getBusinessCalendarDescriptor() {
        return businessCalendarDescriptor;
    }

    public void setBusinessCalendarDescriptor(BusinessCalendarDescriptor businessCalendarDescriptor) {
        this.businessCalendarDescriptor = businessCalendarDescriptor;
    }
}
