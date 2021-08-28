import io.deephaven.grpc_api.appmode.ApplicationContext

import io.deephaven.db.tables.utils.TableTools

def start = { app ->
    size = 42
    app.setField("hello", TableTools.emptyTable(size))
    app.setField("world", TableTools.timeTable("00:00:01"))
}

ApplicationContext.initialize(start)
