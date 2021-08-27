package io.deephaven.lang.parse;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.generated.Chunker;
import io.deephaven.lang.generated.ChunkerDocument;
import io.deephaven.lang.generated.ParseException;
import io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequest;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A specialized parser for autocompletion; maybe better to call it a chunker than a parser...
 */
public class CompletionParser implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletionParser.class);
    private final Map<String, PendingParse> docs = new ConcurrentHashMap<>();

    public ParsedDocument parse(String document) throws ParseException {
        Chunker chunker = new Chunker(document);
        final ChunkerDocument doc = chunker.Document();
        return new ParsedDocument(doc, document);
    }

    public void open(final String text, final String uri, final String version) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace()
                    .append("Opening document ")
                    .append(uri)
                    .append("[")
                    .append(version)
                    .append("] ->\n")
                    .append(text)
                    .endl();
        }
        startParse(uri)
                .requestParse(String.valueOf(version), text, false);
    }

    private PendingParse startParse(String uri) {
        return docs.computeIfAbsent(uri, k -> new PendingParse(uri));
    }

    public void update(final String uri, final String version,
            final List<ChangeDocumentRequest.TextDocumentContentChangeEvent> changes) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace()
                    .append("Updating document ")
                    .append(uri)
                    .append(" [")
                    .append(version)
                    .append("] all docs: ")
                    .append(docs.keySet().toString())
                    .append(" changes: ")
                    .append(changes.toString())
                    .endl();
        }
        PendingParse doc = docs.get(uri);
        final boolean forceParse;
        if (doc == null) {
            doc = startParse(uri);
            forceParse = false;
        } else {
            // let the parser know that we have an incoming change, so it can clear out its worker thread asap
            doc.invalidate();
            forceParse = true;
        }
        String document = doc.getText();
        for (ChangeDocumentRequest.TextDocumentContentChangeEventOrBuilder change : changes) {
            DocumentRange range = change.getRange();
            int length = change.getRangeLength();

            int offset = LspTools.getOffsetFromPosition(document, range.getStart());
            if (offset < 0) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn()
                            .append("Invalid change in document ")
                            .append(uri)
                            .append("[")
                            .append(version)
                            .append("] @")
                            .append(range.getStart().getLine())
                            .append(":")
                            .append(range.getStart().getCharacter())
                            .endl();
                }
                return;
            }

            String prefix = offset > 0 && offset <= document.length() ? document.substring(0, offset) : "";
            String suffix = offset + length < document.length() ? document.substring(offset + length) : "";
            document = prefix + change.getText() + suffix;
        }
        doc.requestParse(version, document, forceParse);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace()
                    .append("Finished updating ")
                    .append(uri)
                    .append(" [")
                    .append(version)
                    .append("]")
                    .endl();
        }
    }

    public void remove(String uri) {
        final PendingParse was = docs.remove(uri);
        if (was != null) {
            was.cancel();
        }
    }

    public ParsedDocument finish(String uri) {
        final PendingParse doc = docs.get(uri);
        if (doc == null) {
            throw new IllegalStateException("Unable to find parsed document " + uri);
        }
        return doc.finishParse()
                .orElseThrow(() -> new IllegalStateException("Unable to complete document parsing for " + uri));
    }

    @Override
    public void close() {
        docs.keySet().forEach(this::remove);
    }
}
