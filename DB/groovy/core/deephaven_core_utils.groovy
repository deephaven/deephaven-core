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
performanceInfo = {
    if (db != null) {
        publishVariable("workerName", db.getWorkerName())
        queryPerformanceLog = db.i2("DbInternal", "QueryPerformanceLog").where("Date=currentDateNy()", "WorkerName=workerName");
        updatePerformanceLog = db.i2("DbInternal", "UpdatePerformanceLog").where("Date=currentDateNy()", "WorkerName=workerName").update("Ratio=EntryIntervalUsage*100/IntervalDuration");
        topOffenders=updatePerformanceLog.where("Ratio > 1.00").sortDescending("Ratio").update("EntryDescription=(!isNull(EntryDescription) && EntryDescription.length() > 100) ? EntryDescription.substring(0, 100) : EntryDescription");
        performanceAggregate = updatePerformanceLog.view("IntervalEndTime", "EntryIntervalUsage", "IntervalDuration").by("IntervalEndTime").view("IntervalEndTime", "IntervalDuration=first(IntervalDuration)", "Ratio=100*sum(EntryIntervalUsage)/IntervalDuration")
    } else {
        publishVariable("workerName", context.getWorkerName())
        queryPerformanceLog = context.i("DbInternal", "QueryPerformanceLog").where("Date=currentDateNy()", "WorkerName=workerName");
        updatePerformanceLog = context.i("DbInternal", "UpdatePerformanceLog").where("Date=currentDateNy()", "WorkerName=workerName").update("Ratio=EntryIntervalUsage*100/IntervalDuration");
        topOffenders=updatePerformanceLog.where("Ratio > 1.00").sortDescending("Ratio").update("EntryDescription=(!isNull(EntryDescription) && EntryDescription.length() > 100) ? EntryDescription.substring(0, 100) : EntryDescription");
        performanceAggregate = updatePerformanceLog.view("IntervalEndTime", "EntryIntervalUsage", "IntervalDuration").by("IntervalEndTime").view("IntervalEndTime", "IntervalDuration=first(IntervalDuration)", "Ratio=100*sum(EntryIntervalUsage)/IntervalDuration")
    }
}


import io.deephaven.db.tables.utils.DBDateTime
import io.deephaven.db.tables.utils.DBTimeUtils
import io.deephaven.db.util.PerformanceQueries

performanceOverview = { workerName = null,  String date = DBTimeUtils.currentDateNy(), Boolean useIntraday = true,
                        String serverHost = null, String workerHostName = null ->
    Map<String,Object> tables;

    PerformanceQueries.PerformanceOverview.QueryBuilder queryBuilder = new PerformanceQueries.PerformanceOverview.QueryBuilder()
            .workerName(workerName)
            .date(date)
            .useIntraday(useIntraday)
            .workerHostName(workerHostName);

    if(!useIntraday && PerformanceQueries.PERF_QUERY_BY_INTERNAL_PARTITION) {
        if(db.getServerHost().equals(serverHost)) {
            serverHost = db.getServerHost().replaceAll("\\.", "_")
        }
        queryBuilder = querybuilder.internalPartition(serverHost)
    } else {
        queryBuilder = queryBuilder.hostname(serverHost)
    }

    tables = db.executeQuery(queryBuilder.build())

    // binding.getVariables().addAll(tables)
    tables.each{ k, v -> binding.setVariable(k,v)}
}

performanceOverviewByName = {String queryName , String queryOwner, String date = DBTimeUtils.currentDateNy(),
                             boolean useIntraday=true, String workerHostName = null, DBDateTime asOfTime =  null ->

    Map<String,Object> tables;

    tables = db.executeQuery(
            new PerformanceQueries.PerformanceOverview.QueryBuilder()
                    .date(date)
                    .queryOwner(queryOwner)
                    .queryName(queryName)
                    .asOfTime(asOfTime)
                    .useIntraday(useIntraday)
                    .workerHostName(workerHostName)
                    .build())

    tables.each{ k, v -> binding.setVariable(k,v)}
}


persistentQueryStatusMonitor = {String startDate = null, String endDate = null ->

    Map<String,Object> tables;

    if (startDate == null && endDate == null){
        tables = db.executeQuery(new PerformanceQueries.PersistentQueryStatusMonitor())
    }
    else if (endDate == null ){
        tables = db.executeQuery(new PerformanceQueries.PersistentQueryStatusMonitor(startDate))
    }
    else{
        tables = db.executeQuery(new PerformanceQueries.PersistentQueryStatusMonitor(startDate,endDate))
    }

    //binding.getVariables().addAll(tables)

    tables.each{ k, v -> binding.setVariable(k,v) }
}

///////////////////// Calendars /////////////////////
import static io.deephaven.util.calendar.Calendars.calendar
import static io.deephaven.util.calendar.Calendars.calendarNames

//todo prefix name with CALENDAR?

publishVariable( "CALENDAR_DEFAULT", calendar())

for( String n : calendarNames() ) {
    publishVariable("CALENDAR_" + n, calendar(n))
}


///////////////////// Plotting /////////////////////

//todo what about these commented out imports?
// import static io.deephaven.db.plot.LineStyle.LineEndStyle
// import static io.deephaven.db.plot.LineStyle.LineJoinStyle
// import static io.deephaven.db.plot.Font.FontStyle
//////////////////// Colors ////////////////////////

import io.deephaven.db.plot.PlotStyle
import io.deephaven.db.plot.axistransformations.AxisTransforms
import io.deephaven.db.plot.colors.ColorMaps

import static io.deephaven.db.plot.PlottingConvenience.*

for( String c : io.deephaven.gui.color.Color.colorNames() ) {
    publishVariable( "COLOR_" + c, io.deephaven.gui.color.Color.valueOf(c) )
}

colorTable = {
    t = emptyTable(1)
            .updateView("Colors = colorNames()")
            .ungroup()
    //todo simplify the following with the improved color branch
            .updateView("Paint = io.deephaven.gui.color.Color.color(Colors).javaColor()")
            .formatColumns("Colors = io.deephaven.db.util.DBColorUtil.bgfga(Paint.getRed(), Paint.getGreen(), Paint.getBlue())")
            .dropColumns("Paint")
}

//////////////////// Count Metrics //////////////////////////

resetMetricsCounts = {
    io.deephaven.db.v2.utils.metrics.MetricsManager.resetCounters()
}

getMetricsCounts = {
    io.deephaven.db.v2.utils.metrics.MetricsManager.getCounters()
}

printMetricsCounts = {
    println(io.deephaven.db.v2.utils.metrics.MetricsManager.getCounters())
}
