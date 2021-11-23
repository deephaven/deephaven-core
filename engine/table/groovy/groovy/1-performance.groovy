package groovy
// Functions to gather performnace data for individual queries.

import io.deephaven.engine.table.impl.utils.PerformanceQueries

queryUpdatePerformanceSet = { int evaluationNumber ->
    tables = PerformanceQueries.queryUpdatePerformanceMap(evaluationNumber)
    tables.each{ name, table -> binding.setVariable(name, table) }
}
