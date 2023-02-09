/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "ChartType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsChartType {
    public static final int XY = ChartDescriptor.ChartType.getXY();
    public static final int PIE = ChartDescriptor.ChartType.getPIE();

    @Deprecated
    public static final int OHLC = ChartDescriptor.ChartType.getOHLC();
    public static final int CATEGORY = ChartDescriptor.ChartType.getCATEGORY();
    public static final int XYZ = ChartDescriptor.ChartType.getXYZ();
    public static final int CATEGORY_3D = ChartDescriptor.ChartType.getCATEGORY_3D();
    public static final int TREEMAP = ChartDescriptor.ChartType.getTREEMAP();
}
