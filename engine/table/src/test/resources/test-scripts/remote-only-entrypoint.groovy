import io.deephaven.engine.context.ExecutionContext
import test.notebook.RemoteOnly

ExecutionContext.getContext().getQueryLibrary().importClass(RemoteOnly.class)

testTable = emptyTable(1).updateView(
    "Version = RemoteOnly.getVersion()",
    "Value = RemoteOnly.getValue()",
    "SourceViaClass = RemoteOnly.getSourceViaClass()"
)
