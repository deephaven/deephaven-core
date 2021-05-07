// Functions to gather performnace data for individual queries.

import io.deephaven.db.v2.utils.PerformanceQueries

queryUpdatePerformanceSet = { int evaluationNumber ->
    tables = PerformanceQueries.queryUpdatePerformanceMap(evaluationNumber)
    tables.each{ name, table -> binding.setVariable(name, table) }
}
