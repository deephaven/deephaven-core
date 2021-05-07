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
