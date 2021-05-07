package io.deephaven.lang.parse;

import io.deephaven.io.logger.Logger;
import io.deephaven.lang.generated.Chunker;
import io.deephaven.lang.generated.ChunkerDocument;
import io.deephaven.lang.generated.ParseException;
import io.deephaven.web.shared.ide.lsp.*;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A specialized parser for autocompletion;
 * maybe better to call it a chunker than a parser...
 */
public class CompletionParser {

    private Map<String, PendingParse> docs = new ConcurrentHashMap<>();

    public ParsedDocument parse(String document) throws ParseException {
        Chunker chunker = new Chunker(document);
        final ChunkerDocument doc = chunker.Document();
        return new ParsedDocument(doc, document);
    }

    public void open(TextDocumentItem textDocument, Logger log) {
        String text = textDocument.text != null ? textDocument.text : "";
        log.info()
                .append("Opening document ")
                .append(textDocument.uri)
                .append("[")
                .append(String.valueOf(textDocument.getVersion()))
                .append("] ->\n")
                .append(text)
                .endl();
        startParse(textDocument.uri, log)
                .requestParse(String.valueOf(textDocument.getVersion()), text, false);
    }

    private PendingParse startParse(String uri, Logger log) {
        return docs.computeIfAbsent(uri, k -> new PendingParse(uri, log));
    }

    public void update(
            VersionedTextDocumentIdentifier textDocument,
            TextDocumentContentChangeEvent[] changes,
            Logger log
    ) {
        log.info()
                .append("Updating document ")
                .append(textDocument.uri)
                .append(" [")
                .append(textDocument.version)
                .append("] all docs: ")
                .append(docs.keySet().toString())
                .append(" changes: ")
                .append(Arrays.toString(changes))
                .endl();
        PendingParse doc = docs.get(textDocument.uri);
        final boolean forceParse;
        if (doc == null) {
            doc = startParse(textDocument.uri, log);
            forceParse = false;
        } else {
            // let the parser know that we have an incoming change, so it can clear out its worker thread asap
            doc.invalidate();
            forceParse = true;
        }
        String document = doc.getText();
        for (TextDocumentContentChangeEvent change : changes) {
            DocumentRange range = change.range;
            int length = change.rangeLength;

            if (range == null) {
                length = document.length();
                range = new DocumentRange();
                range.start = new Position();
                range.start.line = 0;
                range.start.character = 0;
            }

            int offset = DocumentRange.getOffsetFromPosition(document, range.start);
            if (offset < 0) {
                log.warn().append("Invalid change in document ")
                        .append(textDocument.uri)
                        .append("[")
                        .append(textDocument.version)
                        .append("] @")
                        .append(range.start.line)
                        .append(":")
                        .append(range.start.character)
                        .endl();
                return;
            }

            String prefix = offset > 0 && offset <= document.length() ? document.substring(0, offset) : "";
            String suffix = offset + length < document.length() ? document.substring(offset + length) : "";
            document = prefix + change.text + suffix;
        }
        doc.requestParse(Integer.toString(textDocument.version), document, forceParse);
        log.info()
                .append("Finished updating ")
                .append(textDocument.uri)
                .append(" [")
                .append(textDocument.version)
                .append("]")
                .endl();
    }

    public void remove(String uri) {
        final PendingParse was = docs.remove(uri);
        if (was != null) {
            was.cancel();
        }
    }

    public ParsedDocument finish(String uri) {
        final PendingParse doc = docs.get(uri);
        return doc.finishParse().orElseThrow(() -> new IllegalStateException("Unable to get parsed document " + uri));
    }

    public String findText(String uri) {
        final PendingParse doc = docs.get(uri);
        return doc == null ? null : doc.getText();
    }
}
