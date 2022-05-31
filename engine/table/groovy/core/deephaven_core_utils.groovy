/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

__QUERY_NAME__="core"
getDevRootPath = {io.deephaven.configuration.Configuration.getInstance().getDevRootPath()}

source = {fileName ->
    __groovySession.runScript(fileName)
}

sourceOnce = {fileName ->
    __groovySession.runScriptOnce(fileName)
}

scriptImportClass = {c -> __groovySession.addScriptImportClass(c)}
scriptImportStatic = {c -> __groovySession.addScriptImportStatic(c)}

isValidVariableName = {name -> name.matches("^[a-zA-Z_][a-zA-Z_0-9]*")}

publishVariable = { String name, value ->
    if(!isValidVariableName(name)){
        throw new RuntimeException("publishVariable: Attempting to publish an invalid variable name: " + name)
    }

    binding.setVariable(name, value)
}

removeVariable = {name ->
    if(!isValidVariableName(name)){
        throw new RuntimeException("removeVariable: Attempting to remove an invalid variable name: " + name)
    }

    binding.variables.remove(name)
}

///////////////////// Performance /////////////////////

///////////////////// Calendars /////////////////////
import static io.deephaven.time.calendar.Calendars.calendar
import static io.deephaven.time.calendar.Calendars.calendarNames

//todo prefix name with CALENDAR?

publishVariable( "CALENDAR_DEFAULT", calendar())

for( String n : calendarNames() ) {
    publishVariable("CALENDAR_" + n, calendar(n))
}


///////////////////// Plotting /////////////////////

//todo what about these commented out imports?
// import static io.deephaven.plot.LineStyle.LineEndStyle
// import static io.deephaven.plot.LineStyle.LineJoinStyle
// import static io.deephaven.plot.Font.FontStyle
//////////////////// Colors ////////////////////////

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

//////////////////// Count Metrics //////////////////////////

resetMetricsCounts = {
    io.deephaven.util.metrics.MetricsManager.resetCounters()
}

getMetricsCounts = {
    io.deephaven.util.metrics.MetricsManager.getCounters()
}

printMetricsCounts = {
    println(io.deephaven.util.metrics.MetricsManager.getCounters())
}
