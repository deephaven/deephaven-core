//////////////////// Count Metrics //////////////////////////

resetMetricsCounts = {
    io.deephaven.engine.v2.utils.metrics.MetricsManager.resetCounters()
}

getMetricsCounts = {
    io.deephaven.engine.v2.utils.metrics.MetricsManager.getCounters()
}

printMetricsCounts = {
    println(io.deephaven.engine.v2.utils.metrics.MetricsManager.getCounters())
}
