package io.deephaven.engine.util.scripts

import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.util.TableTools

class MyDataModel {
    double x = Math.random()
    double y = Math.random()
}

static def makeTheData(count) {
    ExecutionContext.context.queryLibrary.importClass(MyDataModel)

    return TableTools.emptyTable(count).update("Data=new MyDataModel()")
}
