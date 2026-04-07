import io.deephaven.engine.context.ExecutionContext
import test.notebook.Helper

ExecutionContext.getContext().getQueryLibrary().importClass(Helper.class)

// Test various invocation patterns
println "Static method: " + Helper.getVersion()
println "Static value: " + Helper.getValue()
println "Via inner class: " + Helper.getSourceViaClass()

// Create table using formulas that reference the imported class
testTable = emptyTable(1).updateView(
    "Version = Helper.getVersion()",
    "Value = Helper.getValue()",
    "SourceViaClass = Helper.getSourceViaClass()"
)

println "Table created: " + testTable.toString()
