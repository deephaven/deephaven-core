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
