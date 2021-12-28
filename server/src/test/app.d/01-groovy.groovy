import io.deephaven.appmode.ApplicationContext
import io.deephaven.appmode.ApplicationState

import io.deephaven.engine.util.TableTools

def start = { ApplicationState app ->
    size = 42
    app.setField("hello", TableTools.emptyTable(size))
    app.setField("world", TableTools.timeTable("00:00:01"))
}

ApplicationContext.initialize(start)
