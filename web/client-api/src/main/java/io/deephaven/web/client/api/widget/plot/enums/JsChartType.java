package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.ChartDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "ChartType")
@SuppressWarnings("unusable-by-js")
public class JsChartType {
    public static final ChartDescriptor.ChartType XY = ChartDescriptor.ChartType.XY;
    public static final ChartDescriptor.ChartType PIE = ChartDescriptor.ChartType.PIE;
    public static final ChartDescriptor.ChartType OHLC = ChartDescriptor.ChartType.OHLC;
    public static final ChartDescriptor.ChartType CATEGORY = ChartDescriptor.ChartType.CATEGORY;
    public static final ChartDescriptor.ChartType XYZ = ChartDescriptor.ChartType.XYZ;
    public static final ChartDescriptor.ChartType CATEGORY_3D = ChartDescriptor.ChartType.CATEGORY_3D;
}
