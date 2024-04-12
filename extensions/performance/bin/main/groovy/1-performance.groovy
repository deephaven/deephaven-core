// Functions to gather performance data for individual queries.

import io.deephaven.engine.table.impl.util.PerformanceQueries

queryUpdatePerformanceSet = { int evaluationNumber ->
    tables = PerformanceQueries.queryUpdatePerformanceMap(evaluationNumber)
    tables.each{ name, table -> binding.setVariable(name, table) }
}
