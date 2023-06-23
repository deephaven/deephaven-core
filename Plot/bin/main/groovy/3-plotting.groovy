//////////////////// Plotting /////////////////////

//todo what about these commented out imports?
// import static io.deephaven.plot.LineStyle.LineEndStyle
// import static io.deephaven.plot.LineStyle.LineJoinStyle
// import static io.deephaven.plot.Font.FontStyle
//////////////////// Colors ////////////////////////

import io.deephaven.plot.PlotStyle
import io.deephaven.plot.axistransformations.AxisTransforms
import io.deephaven.plot.colors.ColorMaps

import static io.deephaven.plot.PlottingConvenience.*

for( String c : io.deephaven.gui.color.Color.colorNames() ) {
    publishVariable( "COLOR_" + c, io.deephaven.gui.color.Color.valueOf(c) )
}

colorTable = {
    t = emptyTable(1)
            .updateView("Colors = colorNames()")
            .ungroup()
    //todo simplify the following with the improved color branch
            .updateView("Paint = io.deephaven.gui.color.Color.color(Colors).javaColor()")
            .formatColumns("Colors = io.deephaven.engine.util.ColorUtil.bgfga(Paint.getRed(), Paint.getGreen(), Paint.getBlue())")
            .dropColumns("Paint")
}
