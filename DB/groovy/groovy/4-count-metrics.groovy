//////////////////// Count Metrics //////////////////////////

resetMetricsCounts = {
    io.deephaven.engine.structures.metrics.MetricsManager.resetCounters()
}

getMetricsCounts = {
    io.deephaven.engine.structures.metrics.MetricsManager.getCounters()
}

printMetricsCounts = {
    println(io.deephaven.engine.structures.metrics.MetricsManager.getCounters())
}
