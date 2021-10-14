//////////////////// Plotting /////////////////////

//todo what about these commented out imports?
// import static io.deephaven.engine.plot.LineStyle.LineEndStyle
// import static io.deephaven.engine.plot.LineStyle.LineJoinStyle
// import static io.deephaven.engine.plot.Font.FontStyle
//////////////////// Colors ////////////////////////

import io.deephaven.engine.plot.PlotStyle
import io.deephaven.engine.plot.axistransformations.AxisTransforms
import io.deephaven.engine.plot.colors.ColorMaps

import static io.deephaven.engine.plot.PlottingConvenience.*

for( String c : io.deephaven.gui.color.Color.colorNames() ) {
    publishVariable( "COLOR_" + c, io.deephaven.gui.color.Color.valueOf(c) )
}

colorTable = {
    t = emptyTable(1)
            .updateView("Colors = colorNames()")
            .ungroup()
    //todo simplify the following with the improved color branch
            .updateView("Paint = io.deephaven.gui.color.Color.color(Colors).javaColor()")
            .formatColumns("Colors = io.deephaven.engine.util.DBColorUtil.bgfga(Paint.getRed(), Paint.getGreen(), Paint.getBlue())")
            .dropColumns("Paint")
}
