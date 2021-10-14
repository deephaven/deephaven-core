package io.deephaven.lang.completion

import io.deephaven.engine.util.VariableProvider
import io.deephaven.io.logger.Logger
import io.deephaven.proto.backplane.script.grpc.CompletionItem
import io.deephaven.util.process.ProcessEnvironment
import io.deephaven.lang.parse.ParsedDocument

/**
 */
trait ChunkerCompleterMixin extends ChunkerParseTestMixin {

    Set<CompletionItem> performSearch(ParsedDocument doc, int from, VariableProvider vars) {

        Logger log = ProcessEnvironment.getDefaultLog(CompletionHandler)

        ChunkerCompleter completer = new ChunkerCompleter(log, vars)

        return completer.runCompletion(doc, from)

    }

    List<String> sliceString(int startAt = 0, String val) {
        (startAt..val.length()).collect {val.substring(0, it)}
    }
}
